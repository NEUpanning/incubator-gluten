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
package io.glutenproject.backendsapi

import io.glutenproject.GlutenPlugin

// native library 的基类，native library 需要提供以下 API
trait Backend {
  def name(): String
//  native library build 的相关信息例如 branch version
  def buildInfo(): GlutenPlugin.BackendBuildInfo
// 用于迭代 column batch 数据的迭代器？
  def iteratorApi(): IteratorApi
// 获取 gluten 实现的 Gluten Plan exec(继承自spark plan) ,部分实现了TransformSupport，支持将 spark plan transform为 substrait plan
  def sparkPlanExecApi(): SparkPlanExecApi
// transform 相关接口，例如 postProcessNativeConfig 是将spark的参数转换为native library的参数（spark.executor.cores参数就应用于spark.gluten.sql.columnar.backend.ch.runtime_settings.max_threads）
  def transformerApi(): TransformerApi
// validate spark plan是否可以使用 native library 否则 fallback 到 vanilla spark
  def validatorApi(): ValidatorApi
// metrics 相关
  def metricsApi(): MetricsApi
// driver,executor启动和停止相关 listener
  def listenerApi(): ListenerApi
// 当前是空实现
  def broadcastApi(): BroadcastApi
// settings
  def settings(): BackendSettingsApi
}
