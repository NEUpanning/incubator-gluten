/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.table.planner.plan.nodes.exec.stream;

import org.apache.gluten.rexnode.RexConversionContext;
import org.apache.gluten.rexnode.RexNodeConverter;
import org.apache.gluten.rexnode.Utils;
import org.apache.gluten.table.runtime.operators.GlutenVectorTwoInputOperator;
import org.apache.gluten.util.LogicalTypeConverter;
import org.apache.gluten.util.PlanNodeIdGenerator;

import io.github.zhztheplayer.velox4j.connector.ExternalStreamTableHandle;
import io.github.zhztheplayer.velox4j.expression.CallTypedExpr;
import io.github.zhztheplayer.velox4j.expression.FieldAccessTypedExpr;
import io.github.zhztheplayer.velox4j.expression.TypedExpr;
import io.github.zhztheplayer.velox4j.plan.EmptyNode;
import io.github.zhztheplayer.velox4j.plan.NestedLoopJoinNode;
import io.github.zhztheplayer.velox4j.plan.PlanNode;
import io.github.zhztheplayer.velox4j.plan.StatefulPlanNode;
import io.github.zhztheplayer.velox4j.plan.StreamJoinNode;
import io.github.zhztheplayer.velox4j.plan.TableScanNode;
import io.github.zhztheplayer.velox4j.type.BooleanType;

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.StateMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.spec.JoinSpec;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.utils.JoinUtil;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.planner.plan.utils.MinibatchUtil;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.stream.MiniBatchStreamingJoinOperator;
import org.apache.flink.table.runtime.operators.join.stream.StreamingSemiAntiJoinOperator;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinInputSideSpec;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.guava31.com.google.common.collect.Lists;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.calcite.rex.RexNode;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link StreamExecNode} for regular Joins.
 *
 * <p>Regular joins are the most generic type of join in which any new records or changes to either
 * side of the join input are visible and are affecting the whole join result.
 */
@ExecNodeMetadata(
    name = "stream-exec-join",
    version = 1,
    producedTransformations = StreamExecJoin.JOIN_TRANSFORMATION,
    minPlanVersion = FlinkVersion.v1_15,
    minStateVersion = FlinkVersion.v1_15)
public class StreamExecJoin extends ExecNodeBase<RowData>
    implements StreamExecNode<RowData>, SingleTransformationTranslator<RowData> {

  public static final String JOIN_TRANSFORMATION = "join";

  public static final String LEFT_STATE_NAME = "leftState";

  public static final String RIGHT_STATE_NAME = "rightState";

  public static final String FIELD_NAME_JOIN_SPEC = "joinSpec";
  public static final String FIELD_NAME_LEFT_UPSERT_KEYS = "leftUpsertKeys";
  public static final String FIELD_NAME_RIGHT_UPSERT_KEYS = "rightUpsertKeys";

  @JsonProperty(FIELD_NAME_JOIN_SPEC)
  private final JoinSpec joinSpec;

  @JsonProperty(FIELD_NAME_LEFT_UPSERT_KEYS)
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  private final List<int[]> leftUpsertKeys;

  @JsonProperty(FIELD_NAME_RIGHT_UPSERT_KEYS)
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  private final List<int[]> rightUpsertKeys;

  @Nullable
  @JsonProperty(FIELD_NAME_STATE)
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private final List<StateMetadata> stateMetadataList;

  public StreamExecJoin(
      ReadableConfig tableConfig,
      JoinSpec joinSpec,
      List<int[]> leftUpsertKeys,
      List<int[]> rightUpsertKeys,
      InputProperty leftInputProperty,
      InputProperty rightInputProperty,
      Map<Integer, Long> stateTtlFromHint,
      RowType outputType,
      String description) {
    this(
        ExecNodeContext.newNodeId(),
        ExecNodeContext.newContext(StreamExecJoin.class),
        ExecNodeContext.newPersistedConfig(StreamExecJoin.class, tableConfig),
        joinSpec,
        leftUpsertKeys,
        rightUpsertKeys,
        StateMetadata.getMultiInputOperatorDefaultMeta(
            stateTtlFromHint, tableConfig, LEFT_STATE_NAME, RIGHT_STATE_NAME),
        Lists.newArrayList(leftInputProperty, rightInputProperty),
        outputType,
        description);
  }

  @JsonCreator
  public StreamExecJoin(
      @JsonProperty(FIELD_NAME_ID) int id,
      @JsonProperty(FIELD_NAME_TYPE) ExecNodeContext context,
      @JsonProperty(FIELD_NAME_CONFIGURATION) ReadableConfig persistedConfig,
      @JsonProperty(FIELD_NAME_JOIN_SPEC) JoinSpec joinSpec,
      @JsonProperty(FIELD_NAME_LEFT_UPSERT_KEYS) List<int[]> leftUpsertKeys,
      @JsonProperty(FIELD_NAME_RIGHT_UPSERT_KEYS) List<int[]> rightUpsertKeys,
      @Nullable @JsonProperty(FIELD_NAME_STATE) List<StateMetadata> stateMetadataList,
      @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
      @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
      @JsonProperty(FIELD_NAME_DESCRIPTION) String description) {
    super(id, context, persistedConfig, inputProperties, outputType, description);
    checkArgument(inputProperties.size() == 2);
    this.joinSpec = checkNotNull(joinSpec);
    this.leftUpsertKeys = leftUpsertKeys;
    this.rightUpsertKeys = rightUpsertKeys;
    this.stateMetadataList = stateMetadataList;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Transformation<RowData> translateToPlanInternal(
      PlannerBase planner, ExecNodeConfig config) {
    final ExecEdge leftInputEdge = getInputEdges().get(0);
    final ExecEdge rightInputEdge = getInputEdges().get(1);

    final Transformation<RowData> leftTransform =
        (Transformation<RowData>) leftInputEdge.translateToPlan(planner);
    final Transformation<RowData> rightTransform =
        (Transformation<RowData>) rightInputEdge.translateToPlan(planner);

    final RowType leftType = (RowType) leftInputEdge.getOutputType();
    final RowType rightType = (RowType) rightInputEdge.getOutputType();
    JoinUtil.validateJoinSpec(joinSpec, leftType, rightType, true);

    final int[] leftJoinKey = joinSpec.getLeftKeys();
    final int[] rightJoinKey = joinSpec.getRightKeys();

    final InternalTypeInfo<RowData> leftTypeInfo = InternalTypeInfo.of(leftType);
    final JoinInputSideSpec leftInputSpec =
        JoinUtil.analyzeJoinInput(
            planner.getFlinkContext().getClassLoader(), leftTypeInfo, leftJoinKey, leftUpsertKeys);

    final InternalTypeInfo<RowData> rightTypeInfo = InternalTypeInfo.of(rightType);
    final JoinInputSideSpec rightInputSpec =
        JoinUtil.analyzeJoinInput(
            planner.getFlinkContext().getClassLoader(),
            rightTypeInfo,
            rightJoinKey,
            rightUpsertKeys);

    GeneratedJoinCondition generatedCondition =
        JoinUtil.generateConditionFunction(
            config, planner.getFlinkContext().getClassLoader(), joinSpec, leftType, rightType);

    List<Long> leftAndRightStateRetentionTime =
        StateMetadata.getStateTtlForMultiInputOperator(config, 2, stateMetadataList);
    long leftStateRetentionTime = leftAndRightStateRetentionTime.get(0);
    long rightStateRetentionTime = leftAndRightStateRetentionTime.get(1);

    TwoInputStreamOperator operator;
    FlinkJoinType joinType = joinSpec.getJoinType();
    final boolean isMiniBatchEnabled = MinibatchUtil.isMiniBatchEnabled(config);
    if (joinType == FlinkJoinType.ANTI || joinType == FlinkJoinType.SEMI) {
      operator =
          new StreamingSemiAntiJoinOperator(
              joinType == FlinkJoinType.ANTI,
              leftTypeInfo,
              rightTypeInfo,
              generatedCondition,
              leftInputSpec,
              rightInputSpec,
              joinSpec.getFilterNulls(),
              leftStateRetentionTime,
              rightStateRetentionTime);
    } else {
      boolean leftIsOuter = joinType == FlinkJoinType.LEFT || joinType == FlinkJoinType.FULL;
      boolean rightIsOuter = joinType == FlinkJoinType.RIGHT || joinType == FlinkJoinType.FULL;
      if (isMiniBatchEnabled) {
        operator =
            MiniBatchStreamingJoinOperator.newMiniBatchStreamJoinOperator(
                joinType,
                leftTypeInfo,
                rightTypeInfo,
                generatedCondition,
                leftInputSpec,
                rightInputSpec,
                leftIsOuter,
                rightIsOuter,
                joinSpec.getFilterNulls(),
                leftStateRetentionTime,
                rightStateRetentionTime,
                MinibatchUtil.createMiniBatchCoTrigger(config));
      } else {
        // --- Begin Gluten-specific code changes ---
        io.github.zhztheplayer.velox4j.type.RowType leftInputType =
            (io.github.zhztheplayer.velox4j.type.RowType)
                LogicalTypeConverter.toVLType(leftInputEdge.getOutputType());
        io.github.zhztheplayer.velox4j.type.RowType rightInputType =
            (io.github.zhztheplayer.velox4j.type.RowType)
                LogicalTypeConverter.toVLType(rightInputEdge.getOutputType());
        io.github.zhztheplayer.velox4j.type.RowType outputType =
            (io.github.zhztheplayer.velox4j.type.RowType)
                LogicalTypeConverter.toVLType(getOutputType());
        rightInputType = Utils.substituteSameName(leftInputType, rightInputType, outputType);
        List<FieldAccessTypedExpr> leftKeys =
            Utils.analyzeJoinKeys(leftInputType, leftJoinKey, leftUpsertKeys);
        List<FieldAccessTypedExpr> rightKeys =
            Utils.analyzeJoinKeys(rightInputType, rightJoinKey, rightUpsertKeys);
        TypedExpr joinCondition = Utils.generateJoinEqualCondition(leftKeys, rightKeys);
        RexNode condition = joinSpec.getNonEquiCondition().orElse(null);
        if (condition != null) {
          RexConversionContext conversionContext = new RexConversionContext(outputType.getNames());
          TypedExpr nonEqual = RexNodeConverter.toTypedExpr(condition, conversionContext);
          joinCondition =
              joinCondition == null
                  ? nonEqual
                  : new CallTypedExpr(new BooleanType(), List.of(joinCondition, nonEqual), "and");
        }

        PlanNode leftInput =
            new TableScanNode(
                PlanNodeIdGenerator.newId(),
                leftInputType,
                new ExternalStreamTableHandle("connector-external-stream"),
                List.of());
        PlanNode rightInput =
            new TableScanNode(
                PlanNodeIdGenerator.newId(),
                rightInputType,
                new ExternalStreamTableHandle("connector-external-stream"),
                List.of());
        NestedLoopJoinNode leftNode =
            new NestedLoopJoinNode(
                PlanNodeIdGenerator.newId(),
                Utils.toVLJoinType(joinType),
                joinCondition,
                new EmptyNode(leftInputType),
                new EmptyNode(rightInputType),
                outputType);
        NestedLoopJoinNode rightNode =
            new NestedLoopJoinNode(
                PlanNodeIdGenerator.newId(),
                Utils.toVLJoinType(joinType),
                joinCondition,
                new EmptyNode(rightInputType),
                new EmptyNode(leftInputType),
                outputType);
        PlanNode join =
            new StreamJoinNode(
                PlanNodeIdGenerator.newId(),
                leftInput,
                rightInput,
                leftNode,
                rightNode,
                outputType);
        operator =
            new GlutenVectorTwoInputOperator(
                new StatefulPlanNode(join.getId(), join),
                leftInput.getId(),
                rightInput.getId(),
                leftInputType,
                rightInputType,
                Map.of(join.getId(), outputType));
        // --- End Gluten-specific code changes ---
      }
    }

    final RowType returnType = (RowType) getOutputType();
    final TwoInputTransformation<RowData, RowData, RowData> transform =
        ExecNodeUtil.createTwoInputTransformation(
            leftTransform,
            rightTransform,
            createTransformationMeta(JOIN_TRANSFORMATION, config),
            operator,
            InternalTypeInfo.of(returnType),
            leftTransform.getParallelism(),
            false);

    // set KeyType and Selector for state
    RowDataKeySelector leftSelect =
        KeySelectorUtil.getRowDataSelector(
            planner.getFlinkContext().getClassLoader(), leftJoinKey, leftTypeInfo);
    RowDataKeySelector rightSelect =
        KeySelectorUtil.getRowDataSelector(
            planner.getFlinkContext().getClassLoader(), rightJoinKey, rightTypeInfo);
    transform.setStateKeySelectors(leftSelect, rightSelect);
    transform.setStateKeyType(leftSelect.getProducedType());
    return transform;
  }
}
