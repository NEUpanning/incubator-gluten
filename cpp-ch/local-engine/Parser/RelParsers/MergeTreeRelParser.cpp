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

#include "MergeTreeRelParser.h"
#include <Core/Settings.h>
#include <Parser/ExpressionParser.h>
#include <Parser/FunctionParser.h>
#include <Parser/SubstraitParserUtils.h>
#include <Parser/TypeParser.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Storages/MergeTree/StorageMergeTreeFactory.h>
#include <Storages/SubstraitSource/FormatFile.h>
#include <boost/algorithm/string/case_conv.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <google/protobuf/wrappers.pb.h>
#include <Poco/StringTokenizer.h>
#include <Common/BlockTypeUtils.h>
#include <Common/GlutenSettings.h>
#include <Common/PlanUtil.h>

namespace DB
{
namespace Setting
{
extern const SettingsUInt64 max_block_size;
}
namespace ErrorCodes
{
extern const int NO_SUCH_DATA_PART;
extern const int LOGICAL_ERROR;
extern const int UNKNOWN_FUNCTION;
extern const int UNKNOWN_TYPE;
}
}

namespace local_engine
{
using namespace DB;

/// Find minimal position of the column in primary key.
static Int64 findMinPosition(const NameSet & condition_table_columns, const NameToIndexMap & primary_key_positions)
{
    Int64 min_position = std::numeric_limits<Int64>::max() - 1;

    for (const auto & column : condition_table_columns)
    {
        auto it = primary_key_positions.find(column);
        if (it != primary_key_positions.end())
            min_position = std::min(min_position, static_cast<Int64>(it->second));
    }

    return min_position;
}

DB::QueryPlanPtr MergeTreeRelParser::parseReadRel(
    DB::QueryPlanPtr query_plan, const substrait::ReadRel & rel, const substrait::ReadRel::ExtensionTable & extension_table)
{
    MergeTreeTableInstance merge_tree_table(extension_table);
    // ignore snapshot id for a query
    merge_tree_table.snapshot_id = "";
    auto storage = merge_tree_table.restoreStorage(QueryContext::globalMutableContext());

    DB::Block input;
    DB::Block original_input;
    if (rel.has_base_schema() && rel.base_schema().names_size())
    {
        input = TypeParser::buildBlockFromNamedStruct(rel.base_schema());
        if (input.findByName(FileMetaColumns::INPUT_FILE_NAME) != nullptr)
        {
            // mergetree use concat(path, _part) instead of input_file_name
            input.insert(ColumnWithTypeAndName(ColumnWithTypeAndName(std::make_shared<DataTypeString>(), VIRTUAL_COLUMN_PART)));
        }
        // remove input_file_name, input_file_block_start, input_file_block_size due to mergetree doesn't have them
        input = FileMetaColumns::removeVirtualColumns(input);

        SparkSQLConfig sql_config = SparkSQLConfig::loadFromContext(context);
        // case_insensitive_matching
        if (!sql_config.caseSensitive)
        {
            original_input = input;
            auto all = storage->getInMemoryMetadataPtr()->getColumns().getNamesOfPhysical();
            std::ranges::for_each(
                input,
                [&all](ColumnWithTypeAndName & column)
                {
                    const auto found
                        = std::ranges::find_if(all, [&column](const auto & name) -> bool { return boost::iequals(column.name, name); });
                    if (found != all.end())
                        column.name = *found;
                });
        }
    }
    else
    {
        NamesAndTypesList one_column_name_type;
        one_column_name_type.push_back(storage->getInMemoryMetadataPtr()->getColumns().getAll().front());
        input = toSampleBlock(one_column_name_type);
        LOG_DEBUG(getLogger("SerializedPlanParser"), "Try to read ({}) instead of empty header", one_column_name_type.front().dump());
    }

    std::vector<DataPartPtr> selected_parts = StorageMergeTreeFactory::getDataPartsByNames(
        storage->getStorageID(), merge_tree_table.snapshot_id, merge_tree_table.getPartNames());

    for (const auto & [name, sizes] : storage->getColumnSizes())
        column_sizes[name] = sizes.data_compressed;

    auto storage_snapshot = std::make_shared<StorageSnapshot>(*storage, storage->getInMemoryMetadataPtr());
    auto names_and_types_list = input.getNamesAndTypesList();

    auto query_info = buildQueryInfo(names_and_types_list);

    std::set<String> non_nullable_columns;
    if (rel.has_filter())
    {
        NonNullableColumnsResolver non_nullable_columns_resolver(input, parser_context, rel.filter());
        non_nullable_columns = non_nullable_columns_resolver.resolve();
        query_info->prewhere_info = parsePreWhereInfo(rel.filter(), input);
    }

    auto read_step = storage->reader.readFromParts(
        selected_parts,
        storage->getMutationsSnapshot({}),
        names_and_types_list.getNames(),
        storage_snapshot,
        *query_info,
        context,
        context->getSettingsRef()[Setting::max_block_size],
        1);

    auto * source_step_with_filter = static_cast<SourceStepWithFilterBase *>(read_step.get());
    if (const auto & storage_prewhere_info = query_info->prewhere_info)
    {
        source_step_with_filter->addFilter(storage_prewhere_info->prewhere_actions.clone(), storage_prewhere_info->prewhere_column_name);
        source_step_with_filter->applyFilters();
    }

    auto ranges = merge_tree_table.extractRange(selected_parts);
    if (settingsEqual(context->getSettingsRef(), "enabled_driver_filter_mergetree_index", "true"))
        SparkStorageMergeTree::analysisPartsByRanges(*reinterpret_cast<ReadFromMergeTree *>(read_step.get()), ranges);
    else
        SparkStorageMergeTree::wrapRangesInDataParts(*reinterpret_cast<ReadFromMergeTree *>(read_step.get()), ranges);

    steps.emplace_back(read_step.get());
    query_plan->addStep(std::move(read_step));
    if (!non_nullable_columns.empty())
    {
        auto input_header = query_plan->getCurrentHeader();
        std::erase_if(non_nullable_columns, [input_header](auto item) -> bool { return !input_header.has(item); });
        if (auto * remove_null_step = PlanUtil::addRemoveNullableStep(*query_plan, parser_context->queryContext(), non_nullable_columns))
            steps.emplace_back(remove_null_step);
    }

    if (original_input.columns() == input.columns() && !sameName(input, original_input))
    {
        steps.emplace_back(PlanUtil::renamePlanHeader(
            *query_plan,
            [&original_input](const Block & input, NamesWithAliases & aliases) { aliases = buildNamesWithAliases(input, original_input); },
            "Rename MergeTree Output"));
    }

    // add virtual columns step
    if (auto step = MergeTreeRelParser::addVirtualColumnsProjectStep(*query_plan, rel, merge_tree_table.absolute_path); step.has_value())
        steps.emplace_back(step.value());
    return query_plan;
}

std::optional<DB::IQueryPlanStep *> MergeTreeRelParser::addVirtualColumnsProjectStep(DB::QueryPlan & plan, const substrait::ReadRel & rel, const std::string & path)
{
    if (!rel.has_base_schema() || rel.base_schema().names_size() < 1)
        return std::nullopt;
    bool contains_input_file_name = false;
    bool contains_input_file_block_start = false;
    bool contains_input_file_block_length = false;
    for (const auto & name : rel.base_schema().names())
    {
        if (name == FileMetaColumns::INPUT_FILE_NAME)
            contains_input_file_name = true;
        if (name == FileMetaColumns::INPUT_FILE_BLOCK_START)
            contains_input_file_block_start = true;
        if (name == FileMetaColumns::INPUT_FILE_BLOCK_LENGTH)
            contains_input_file_block_length = true;
    }
    if (!contains_input_file_name && !contains_input_file_block_start && !contains_input_file_block_length)
        return std::nullopt;

    const auto & header = plan.getCurrentHeader();
    DB::ActionsDAG actions_dag(header.getNamesAndTypesList());
    if (contains_input_file_name)
    {
        auto concat_func = FunctionFactory::instance().get("concat", context);
        DB::ActionsDAG::NodeRawConstPtrs args;
        const auto string_type = std::make_shared<DB::DataTypeString>();
        const auto * path_node = &actions_dag.addColumn(DB::ColumnWithTypeAndName(string_type->createColumnConst(1, path + "/"), string_type, "path"));
        args.emplace_back(path_node);
        const auto & part_name = actions_dag.findInOutputs(VIRTUAL_COLUMN_PART);
        args.emplace_back(&part_name);
        actions_dag.addOrReplaceInOutputs(actions_dag.addFunction(concat_func, args, FileMetaColumns::INPUT_FILE_NAME));
    }
    if (contains_input_file_block_start)
    {
        const auto int64_type = std::make_shared<DB::DataTypeInt64>();
        actions_dag.addOrReplaceInOutputs(actions_dag.addColumn(DB::ColumnWithTypeAndName(int64_type->createColumnConst(1, -1), int64_type, FileMetaColumns::INPUT_FILE_BLOCK_START)));
    }
    if (contains_input_file_block_length)
    {
        const auto int64_type = std::make_shared<DB::DataTypeInt64>();
        actions_dag.addOrReplaceInOutputs(actions_dag.addColumn(DB::ColumnWithTypeAndName(int64_type->createColumnConst(1, -1), int64_type, FileMetaColumns::INPUT_FILE_BLOCK_LENGTH)));
    }

    if (contains_input_file_name)
        actions_dag.removeUnusedResult(VIRTUAL_COLUMN_PART);
    auto step = std::make_unique<DB::ExpressionStep>(plan.getCurrentHeader(), std::move(actions_dag));
    step->setStepDescription("Add virtual columns");
    std::optional<DB::IQueryPlanStep *> result = step.get();
    plan.addStep(std::move(step));
    return result;
}



PrewhereInfoPtr MergeTreeRelParser::parsePreWhereInfo(const substrait::Expression & rel, const Block & input)
{
    std::string filter_name;
    auto prewhere_info = std::make_shared<PrewhereInfo>();
    prewhere_info->prewhere_actions = optimizePrewhereAction(rel, filter_name, input);
    prewhere_info->prewhere_column_name = filter_name;
    prewhere_info->need_filter = true;
    prewhere_info->remove_prewhere_column = true;

    for (const auto & name : input.getNames())
        prewhere_info->prewhere_actions.tryRestoreColumn(name);
    return prewhere_info;
}

DB::ActionsDAG MergeTreeRelParser::optimizePrewhereAction(const substrait::Expression & rel, std::string & filter_name, const Block & block)
{
    Conditions res;
    std::set<Int64> pk_positions;
    analyzeExpressions(res, rel, pk_positions, block);

    Int64 min_valid_pk_pos = -1;
    for (auto pk_pos : pk_positions)
    {
        if (pk_pos != min_valid_pk_pos + 1)
            break;
        min_valid_pk_pos = pk_pos;
    }

    // TODO need to test
    for (auto & cond : res)
        if (cond.min_position_in_primary_key > min_valid_pk_pos)
            cond.min_position_in_primary_key = std::numeric_limits<Int64>::max() - 1;

    // filter less size column first
    res.sort();
    ActionsDAG filter_action{block.getNamesAndTypesList()};

    if (res.size() == 1)
    {
        parseToAction(filter_action, res.back().node, filter_name);
    }
    else
    {
        DB::ActionsDAG::NodeRawConstPtrs args;

        for (const Condition & cond : res)
        {
            String ignore;
            parseToAction(filter_action, cond.node, ignore);
            args.emplace_back(&filter_action.getNodes().back());
        }

        auto function_builder = FunctionFactory::instance().get("and", context);
        std::string args_name = join(args, ',');
        filter_name = +"and(" + args_name + ")";
        const auto * and_function = &filter_action.addFunction(function_builder, args, filter_name);
        filter_action.addOrReplaceInOutputs(*and_function);
    }

    filter_action.removeUnusedActions(Names{filter_name}, false, true);
    return filter_action;
}

void MergeTreeRelParser::parseToAction(ActionsDAG & filter_action, const substrait::Expression & rel, std::string & filter_name) const
{
    if (rel.has_scalar_function())
    {
        const auto * node = expression_parser->parseFunction(rel.scalar_function(), filter_action, true);
        filter_name = node->result_name;
    }
    else
    {
        const auto * in_node = parseExpression(filter_action, rel);
        filter_action.addOrReplaceInOutputs(*in_node);
        filter_name = in_node->result_name;
    }
}

void MergeTreeRelParser::analyzeExpressions(
    Conditions & res, const substrait::Expression & rel, std::set<Int64> & pk_positions, const Block & block)
{
    if (rel.has_scalar_function() && getCHFunctionName(rel.scalar_function()) == "and")
    {
        int arguments_size = rel.scalar_function().arguments_size();

        for (int i = 0; i < arguments_size; ++i)
        {
            auto argument = rel.scalar_function().arguments(i);
            analyzeExpressions(res, argument.value(), pk_positions, block);
        }
    }
    else
    {
        Condition cond(rel);
        collectColumns(rel, cond.table_columns, block);
        cond.columns_size = getColumnsSize(cond.table_columns);

        // TODO: get primary_key_names
        const NameToIndexMap primary_key_names_positions;
        cond.min_position_in_primary_key = findMinPosition(cond.table_columns, primary_key_names_positions);
        pk_positions.emplace(cond.min_position_in_primary_key);

        res.emplace_back(std::move(cond));
    }
}


UInt64 MergeTreeRelParser::getColumnsSize(const NameSet & columns)
{
    UInt64 size = 0;
    for (const auto & column : columns)
        if (column_sizes.contains(column))
            size += column_sizes[column];

    return size;
}

void MergeTreeRelParser::collectColumns(const substrait::Expression & rel, NameSet & columns, const Block & block)
{
    switch (rel.rex_type_case())
    {
        case substrait::Expression::RexTypeCase::kLiteral: {
            return;
        }

        case substrait::Expression::RexTypeCase::kSelection: {
            auto idx = SubstraitParserUtils::getStructFieldIndex(rel);
            if (!idx)
                throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Selection node must have direct reference.");
            if (const Names names = block.getNames(); names.size() > *idx)
                columns.insert(names[*idx]);

            return;
        }

        case substrait::Expression::RexTypeCase::kCast: {
            const auto & input = rel.cast().input();
            collectColumns(input, columns, block);
            return;
        }

        case substrait::Expression::RexTypeCase::kIfThen: {
            const auto & if_then = rel.if_then();

            auto condition_nums = if_then.ifs_size();
            for (int i = 0; i < condition_nums; ++i)
            {
                const auto & ifs = if_then.ifs(i);
                collectColumns(ifs.if_(), columns, block);
                collectColumns(ifs.then(), columns, block);
            }

            return;
        }

        case substrait::Expression::RexTypeCase::kScalarFunction: {
            for (const auto & arg : rel.scalar_function().arguments())
                collectColumns(arg.value(), columns, block);

            return;
        }

        case substrait::Expression::RexTypeCase::kSingularOrList: {
            const auto & options = rel.singular_or_list().options();
            /// options is empty always return false
            if (options.empty())
                return;

            collectColumns(rel.singular_or_list().value(), columns, block);
            return;
        }

        default:
            throw Exception(
                ErrorCodes::UNKNOWN_TYPE,
                "Unsupported spark expression type {} : {}",
                magic_enum::enum_name(rel.rex_type_case()),
                rel.DebugString());
    }
}

String MergeTreeRelParser::getCHFunctionName(const substrait::Expression_ScalarFunction & substrait_func) const
{
    return expression_parser->getFunctionName(substrait_func);
}

String MergeTreeRelParser::filterRangesOnDriver(const substrait::ReadRel & read_rel)
{
    MergeTreeTableInstance merge_tree_table(read_rel.advanced_extension().enhancement());
    // ignore snapshot id for query
    merge_tree_table.snapshot_id = "";
    auto storage = merge_tree_table.restoreStorage(QueryContext::globalMutableContext());

    auto input = TypeParser::buildBlockFromNamedStruct(read_rel.base_schema());
    auto names_and_types_list = input.getNamesAndTypesList();
    auto query_info = buildQueryInfo(names_and_types_list);

    query_info->prewhere_info = parsePreWhereInfo(read_rel.filter(), input);

    std::vector<DataPartPtr> selected_parts = StorageMergeTreeFactory::getDataPartsByNames(
        StorageID(merge_tree_table.database, merge_tree_table.table), merge_tree_table.snapshot_id, merge_tree_table.getPartNames());

    auto storage_snapshot = std::make_shared<StorageSnapshot>(*storage, storage->getInMemoryMetadataPtr());
    if (selected_parts.empty())
        throw Exception(ErrorCodes::NO_SUCH_DATA_PART, "no data part found.");
    auto read_step = storage->reader.readFromParts(
        selected_parts,
        /* alter_conversions = */
        {},
        names_and_types_list.getNames(),
        storage_snapshot,
        *query_info,
        context,
        context->getSettingsRef()[Setting::max_block_size],
        10); // TODO: Expect use driver cores.

    auto * read_from_mergetree = static_cast<ReadFromMergeTree *>(read_step.get());
    if (const auto & storage_prewhere_info = query_info->prewhere_info)
    {
        ActionDAGNodes filter_nodes;
        filter_nodes.nodes.emplace_back(
            &storage_prewhere_info->prewhere_actions.findInOutputs(storage_prewhere_info->prewhere_column_name));
        read_from_mergetree->applyFilters(std::move(filter_nodes));
    }

    auto analysis = read_from_mergetree->getAnalysisResult();
    rapidjson::StringBuffer result;
    rapidjson::Writer<rapidjson::StringBuffer> writer(result);
    writer.StartArray();
    for (auto & parts_with_range : analysis.parts_with_ranges)
    {
        MarkRanges final_ranges;
        for (auto & range : parts_with_range.ranges)
        {
            writer.StartObject();
            writer.Key("part_name");
            writer.String(parts_with_range.data_part->name.c_str());
            writer.Key("begin");
            writer.Uint(range.begin);
            writer.Key("end");
            writer.Uint(range.end);
            writer.EndObject();
        }
    }

    writer.EndArray();
    return result.GetString();
}

}
