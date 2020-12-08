/*
 Navicat Premium Data Transfer

 Source Server         : hdspdev007
 Source Server Type    : MySQL
 Source Server Version : 50730
 Source Host           : 172.23.16.63:23306
 Source Schema         : hdsp_factory

 Target Server Type    : MySQL
 Target Server Version : 50730
 File Encoding         : 65001

 Date: 03/12/2020 16:20:03
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for xadt_comparison_job
-- ----------------------------
DROP TABLE IF EXISTS `xadt_comparison_job`;
CREATE TABLE `xadt_comparison_job`  (
                                        `job_id` bigint(20) NOT NULL AUTO_INCREMENT,
                                        `group_code` varchar(30) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL DEFAULT NULL COMMENT '任务组名称',
                                        `job_code` varchar(30) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL COMMENT '任务名称，英文数字下划线',
                                        `job_desc` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL DEFAULT NULL COMMENT '任务描述',
                                        `job_mode` varchar(30) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL COMMENT '任务模式(OPTION:页面向导/IMPORT:脚本配置即自行编写配置文件或自行上传配置文件)',
                                        `app_conf` text CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL COMMENT '数据稽核任务json配置文件',
                                        `result_statistics` text CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL COMMENT '数据稽核结果统计',
                                        `start_time` datetime(0) NULL DEFAULT NULL COMMENT '任务执行开始时间',
                                        `status` varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL DEFAULT 'NEW' COMMENT '任务状态',
                                        `error_msg` text CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL COMMENT '错误信息',
                                        `execute_time` varchar(30) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL DEFAULT NULL COMMENT '任务执行时长',
                                        `tenant_id` bigint(20) NOT NULL DEFAULT 0 COMMENT '租户ID',
                                        `object_version_number` bigint(20) NOT NULL DEFAULT 1 COMMENT '行版本号，用来处理锁',
                                        `creation_date` datetime(0) NOT NULL DEFAULT CURRENT_TIMESTAMP(0),
                                        `created_by` int(11) NOT NULL DEFAULT -1,
                                        `last_updated_by` int(11) NOT NULL DEFAULT -1,
                                        `last_update_date` datetime(0) NOT NULL DEFAULT CURRENT_TIMESTAMP(0),
                                        PRIMARY KEY (`job_id`) USING BTREE,
                                        INDEX `xadt_comparison_job_u1`(`group_code`, `tenant_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_bin ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for xadt_comparison_job_group
-- ----------------------------
DROP TABLE IF EXISTS `xadt_comparison_job_group`;
CREATE TABLE `xadt_comparison_job_group`  (
                                              `group_id` bigint(20) NOT NULL AUTO_INCREMENT,
                                              `group_code` varchar(30) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL COMMENT '任务组名称，英文数字下划线',
                                              `group_desc` varchar(127) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL DEFAULT NULL COMMENT '任务组描述',
                                              `source_datasource_code` varchar(60) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL COMMENT '源数据源',
                                              `target_datasource_code` varchar(60) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL COMMENT '目标数据源',
                                              `source_schema` varchar(40) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL COMMENT '源库',
                                              `source_where` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL DEFAULT NULL COMMENT '库下所有来源表的过滤条件',
                                              `target_schema` varchar(40) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL COMMENT '目标库',
                                              `target_where` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL DEFAULT NULL COMMENT '库下所有目标表的过滤条件',
                                              `tenant_id` bigint(20) NOT NULL DEFAULT 0 COMMENT '租户ID',
                                              `object_version_number` bigint(20) NOT NULL DEFAULT 1 COMMENT '行版本号，用来处理锁',
                                              `creation_date` datetime(0) NOT NULL DEFAULT CURRENT_TIMESTAMP(0),
                                              `created_by` int(11) NOT NULL DEFAULT -1,
                                              `last_updated_by` int(11) NOT NULL DEFAULT -1,
                                              `last_update_date` datetime(0) NOT NULL DEFAULT CURRENT_TIMESTAMP(0),
                                              PRIMARY KEY (`group_id`) USING BTREE,
                                              INDEX `xadt_job_group_u1`(`group_code`, `tenant_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_bin ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for xadt_presto_catalog
-- ----------------------------
DROP TABLE IF EXISTS `xadt_presto_catalog`;
CREATE TABLE `xadt_presto_catalog`  (
                                        `id` bigint(20) NOT NULL AUTO_INCREMENT,
                                        `cluster_code` varchar(30) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL COMMENT '对应presto_cluster表cluster_code',
                                        `catalog_name` varchar(30) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
                                        `connector_name` varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
                                        `properties` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL COMMENT 'json字符串',
                                        `tenant_id` bigint(20) NULL DEFAULT 0 COMMENT '租户ID',
                                        `object_version_number` bigint(20) NOT NULL DEFAULT 1 COMMENT '版本号',
                                        `creation_date` datetime(0) NOT NULL DEFAULT CURRENT_TIMESTAMP(0),
                                        `created_by` int(11) NOT NULL DEFAULT -1,
                                        `last_updated_by` int(11) NOT NULL DEFAULT -1,
                                        `last_update_date` datetime(0) NOT NULL DEFAULT CURRENT_TIMESTAMP(0),
                                        PRIMARY KEY (`id`) USING BTREE,
                                        INDEX `xadt_presto_catalog_u1`(`cluster_code`, `catalog_name`, `tenant_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_bin ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for xadt_presto_cluster
-- ----------------------------
DROP TABLE IF EXISTS `xadt_presto_cluster`;
CREATE TABLE `xadt_presto_cluster`  (
                                        `id` bigint(20) NOT NULL AUTO_INCREMENT,
                                        `cluster_code` varchar(30) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
                                        `datasource_code` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL DEFAULT NULL COMMENT '最好与数据源表对应，即该cluster与presto数据源映射',
                                        `cluster_desc` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL DEFAULT NULL,
                                        `coordinator_url` varchar(120) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
                                        `username` varchar(30) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL DEFAULT 'presto' COMMENT '默认presto',
                                        `enabled_flag` tinyint(1) NULL DEFAULT 1,
                                        `tenant_id` bigint(20) NULL DEFAULT 0 COMMENT '租户ID',
                                        `object_version_number` bigint(20) NOT NULL DEFAULT 1 COMMENT '版本号',
                                        `creation_date` datetime(0) NOT NULL DEFAULT CURRENT_TIMESTAMP(0),
                                        `created_by` int(11) NOT NULL DEFAULT -1,
                                        `last_updated_by` int(11) NOT NULL DEFAULT -1,
                                        `last_update_date` datetime(0) NOT NULL DEFAULT CURRENT_TIMESTAMP(0),
                                        PRIMARY KEY (`id`) USING BTREE,
                                        INDEX `xadt_presto_cluster_u1`(`cluster_code`, `tenant_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_bin ROW_FORMAT = Dynamic;

SET FOREIGN_KEY_CHECKS = 1;
