PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  username TEXT NOT NULL,
  password TEXT NOT NULL,
  created_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)),
  updated_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)),
  is_deleted INTEGER NOT NULL DEFAULT 0
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_users_username_alive ON users(username) WHERE is_deleted = 0;

CREATE TABLE IF NOT EXISTS drive_accounts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  name TEXT NOT NULL,
  drive_type TEXT NOT NULL,
  cookie TEXT NOT NULL,
  enabled INTEGER NOT NULL DEFAULT 1,
  is_default INTEGER NOT NULL DEFAULT 0,
  token_updated_at INTEGER NOT NULL DEFAULT 0,
  created_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)),
  updated_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)),
  is_deleted INTEGER NOT NULL DEFAULT 0,
  FOREIGN KEY(user_id) REFERENCES users(id)
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_drive_accounts_user_name_type_alive
  ON drive_accounts(user_id, name, drive_type) WHERE is_deleted = 0;
CREATE INDEX IF NOT EXISTS ix_drive_accounts_user_type_enabled
  ON drive_accounts(user_id, drive_type, enabled, is_deleted);

CREATE TABLE IF NOT EXISTS tasks (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  task_uid TEXT,
  taskname TEXT NOT NULL,
  shareurl TEXT NOT NULL,
  savepath TEXT NOT NULL,
  pattern TEXT,
  replace TEXT,
  enddate TEXT,
  ignore_extension INTEGER NOT NULL DEFAULT 0,
  sort_index INTEGER,
  startfid TEXT,
  account_name TEXT,
  update_subdir TEXT,
  addition_json TEXT,
  extra_json TEXT,
  enabled INTEGER NOT NULL DEFAULT 1,
  created_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)),
  updated_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)),
  is_deleted INTEGER NOT NULL DEFAULT 0,
  FOREIGN KEY(user_id) REFERENCES users(id)
);
CREATE INDEX IF NOT EXISTS ix_tasks_user_enabled ON tasks(user_id, enabled, is_deleted);
CREATE INDEX IF NOT EXISTS ix_tasks_user_taskname ON tasks(user_id, taskname);
CREATE INDEX IF NOT EXISTS ix_tasks_shareurl ON tasks(shareurl);

CREATE TABLE IF NOT EXISTS task_runweek (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  task_id INTEGER NOT NULL,
  weekday INTEGER NOT NULL,
  created_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)),
  updated_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)),
  is_deleted INTEGER NOT NULL DEFAULT 0,
  FOREIGN KEY(task_id) REFERENCES tasks(id)
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_task_runweek_task_weekday_alive
  ON task_runweek(task_id, weekday) WHERE is_deleted = 0;
CREATE INDEX IF NOT EXISTS ix_task_runweek_task ON task_runweek(task_id, is_deleted);

CREATE TABLE IF NOT EXISTS regex_rules (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  name TEXT NOT NULL,
  pattern TEXT NOT NULL,
  replace TEXT NOT NULL,
  created_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)),
  updated_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)),
  is_deleted INTEGER NOT NULL DEFAULT 0,
  FOREIGN KEY(user_id) REFERENCES users(id)
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_regex_rules_user_name_alive
  ON regex_rules(user_id, name) WHERE is_deleted = 0;

CREATE TABLE IF NOT EXISTS plugin_configs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  plugin_key TEXT NOT NULL,
  enabled INTEGER NOT NULL DEFAULT 1,
  config_json TEXT,
  created_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)),
  updated_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)),
  is_deleted INTEGER NOT NULL DEFAULT 0,
  FOREIGN KEY(user_id) REFERENCES users(id)
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_plugin_configs_user_key_alive
  ON plugin_configs(user_id, plugin_key) WHERE is_deleted = 0;

CREATE TABLE IF NOT EXISTS notification_channels (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  channel_key TEXT NOT NULL,
  enabled INTEGER NOT NULL DEFAULT 1,
  config_json TEXT,
  created_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)),
  updated_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)),
  is_deleted INTEGER NOT NULL DEFAULT 0,
  FOREIGN KEY(user_id) REFERENCES users(id)
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_notification_channels_user_key_alive
  ON notification_channels(user_id, channel_key) WHERE is_deleted = 0;

CREATE TABLE IF NOT EXISTS notification_templates (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  template_key TEXT NOT NULL,
  title_template TEXT,
  body_template TEXT,
  channel_key TEXT,
  created_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)),
  updated_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)),
  is_deleted INTEGER NOT NULL DEFAULT 0,
  FOREIGN KEY(user_id) REFERENCES users(id)
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_notification_templates_user_key_alive
  ON notification_templates(user_id, template_key) WHERE is_deleted = 0;
CREATE INDEX IF NOT EXISTS ix_notification_templates_channel ON notification_templates(user_id, channel_key, is_deleted);

CREATE TABLE IF NOT EXISTS data_sources (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  source_key TEXT NOT NULL,
  enabled INTEGER NOT NULL DEFAULT 1,
  config_json TEXT,
  created_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)),
  updated_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)),
  is_deleted INTEGER NOT NULL DEFAULT 0,
  FOREIGN KEY(user_id) REFERENCES users(id)
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_data_sources_user_key_alive
  ON data_sources(user_id, source_key) WHERE is_deleted = 0;

CREATE TABLE IF NOT EXISTS sync_task_configs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  task_id TEXT NOT NULL,
  taskname TEXT,
  source_dir TEXT NOT NULL,
  dest_dir TEXT NOT NULL,
  sync_mode TEXT,
  match_mode TEXT,
  cron TEXT,
  enabled INTEGER NOT NULL DEFAULT 1,
  regex_filter TEXT,
  file_type_filter_json TEXT,
  exclude_empty_dirs INTEGER NOT NULL DEFAULT 0,
  notify_on_complete INTEGER NOT NULL DEFAULT 1,
  notify_on_error INTEGER NOT NULL DEFAULT 1,
  extra_json TEXT,
  created_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)),
  updated_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)),
  is_deleted INTEGER NOT NULL DEFAULT 0,
  FOREIGN KEY(user_id) REFERENCES users(id)
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_sync_task_configs_user_taskid_alive
  ON sync_task_configs(user_id, task_id) WHERE is_deleted = 0;
CREATE INDEX IF NOT EXISTS ix_sync_task_configs_user_enabled ON sync_task_configs(user_id, enabled, is_deleted);

CREATE TABLE IF NOT EXISTS system_settings (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  key TEXT NOT NULL,
  value TEXT NOT NULL,
  created_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)),
  updated_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)),
  is_deleted INTEGER NOT NULL DEFAULT 0,
  FOREIGN KEY(user_id) REFERENCES users(id)
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_system_settings_user_key_alive
  ON system_settings(user_id, key) WHERE is_deleted = 0;

CREATE TABLE IF NOT EXISTS schema_comments (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  object_type TEXT NOT NULL,
  table_name TEXT NOT NULL,
  column_name TEXT NOT NULL DEFAULT '',
  comment TEXT NOT NULL,
  created_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)),
  updated_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)),
  is_deleted INTEGER NOT NULL DEFAULT 0
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_schema_comments_obj_alive
  ON schema_comments(object_type, table_name, column_name) WHERE is_deleted = 0;

INSERT OR IGNORE INTO schema_comments (object_type, table_name, column_name, comment)
VALUES
  ('table','users','', 'WebUI 用户表'),
  ('column','users','id','主键'),
  ('column','users','username','用户名（唯一，逻辑删除后可复用）'),
  ('column','users','password','密码（为兼容旧行为暂存明文；可后续迁移为哈希）'),
  ('column','users','created_at','创建时间戳（秒）'),
  ('column','users','updated_at','更新时间戳（秒）'),
  ('column','users','is_deleted','逻辑删除标记（0/1）');

INSERT OR IGNORE INTO schema_comments (object_type, table_name, column_name, comment)
VALUES
  ('table','drive_accounts','', '网盘账户表（Cookie/refresh_token 存储）'),
  ('column','drive_accounts','id','主键'),
  ('column','drive_accounts','user_id','所属用户ID'),
  ('column','drive_accounts','name','账户名称（用户可读）'),
  ('column','drive_accounts','drive_type','网盘类型（quark/uc/115/baidu/aliyun/xunlei 等）'),
  ('column','drive_accounts','cookie','Cookie 或 refresh_token（敏感信息）'),
  ('column','drive_accounts','enabled','是否启用（0/1）'),
  ('column','drive_accounts','is_default','是否默认账户（0/1）'),
  ('column','drive_accounts','token_updated_at','token 更新时间戳（秒，用于防回滚）'),
  ('column','drive_accounts','created_at','创建时间戳（秒）'),
  ('column','drive_accounts','updated_at','更新时间戳（秒）'),
  ('column','drive_accounts','is_deleted','逻辑删除标记（0/1）');

INSERT OR IGNORE INTO schema_comments (object_type, table_name, column_name, comment)
VALUES
  ('table','tasks','', '转存任务表（对应 tasklist）'),
  ('column','tasks','id','主键'),
  ('column','tasks','user_id','所属用户ID'),
  ('column','tasks','task_uid','任务唯一ID（前端生成并随保存携带，用于稳定识别任务）'),
  ('column','tasks','taskname','任务名称'),
  ('column','tasks','shareurl','分享链接'),
  ('column','tasks','savepath','保存路径'),
  ('column','tasks','pattern','匹配表达式（可为 magic_regex 关键字或原始正则）'),
  ('column','tasks','replace','替换表达式'),
  ('column','tasks','enddate','结束日期（YYYY-MM-DD）'),
  ('column','tasks','ignore_extension','忽略后缀（0/1）'),
  ('column','tasks','sort_index','排序基数'),
  ('column','tasks','startfid','文件起始 fid'),
  ('column','tasks','account_name','使用账户名（为空表示自动选择）'),
  ('column','tasks','update_subdir','更子目录规则'),
  ('column','tasks','addition_json','任务级插件配置（JSON）'),
  ('column','tasks','extra_json','扩展字段（JSON，保留未知字段）'),
  ('column','tasks','enabled','是否启用（0/1）'),
  ('column','tasks','created_at','创建时间戳（秒）'),
  ('column','tasks','updated_at','更新时间戳（秒）'),
  ('column','tasks','is_deleted','逻辑删除标记（0/1）');

INSERT OR IGNORE INTO schema_comments (object_type, table_name, column_name, comment)
VALUES
  ('index','tasks','ux_tasks_user_task_uid_alive','唯一索引：同一用户下存活任务 task_uid 唯一'),
  ('index','tasks','ix_tasks_user_task_uid','索引：按 user_id+task_uid 查询加速');

INSERT OR IGNORE INTO schema_comments (object_type, table_name, column_name, comment)
VALUES
  ('table','task_runweek','', '任务运行周配置表（缺省无记录表示无限制）'),
  ('column','task_runweek','id','主键'),
  ('column','task_runweek','task_id','任务ID'),
  ('column','task_runweek','weekday','星期（1-7）'),
  ('column','task_runweek','created_at','创建时间戳（秒）'),
  ('column','task_runweek','updated_at','更新时间戳（秒）'),
  ('column','task_runweek','is_deleted','逻辑删除标记（0/1）');

INSERT OR IGNORE INTO schema_comments (object_type, table_name, column_name, comment)
VALUES
  ('table','regex_rules','', '正则规则表（magic_regex）'),
  ('column','regex_rules','id','主键'),
  ('column','regex_rules','user_id','所属用户ID'),
  ('column','regex_rules','name','规则名称/关键字（如 $TV_REGEX）'),
  ('column','regex_rules','pattern','匹配表达式'),
  ('column','regex_rules','replace','替换表达式'),
  ('column','regex_rules','created_at','创建时间戳（秒）'),
  ('column','regex_rules','updated_at','更新时间戳（秒）'),
  ('column','regex_rules','is_deleted','逻辑删除标记（0/1）');

INSERT OR IGNORE INTO schema_comments (object_type, table_name, column_name, comment)
VALUES
  ('table','plugin_configs','', '插件配置表（plugins）'),
  ('column','plugin_configs','id','主键'),
  ('column','plugin_configs','user_id','所属用户ID'),
  ('column','plugin_configs','plugin_key','插件键（如 emby/alist/aria2 等）'),
  ('column','plugin_configs','enabled','是否启用（0/1）'),
  ('column','plugin_configs','config_json','插件参数（JSON）'),
  ('column','plugin_configs','created_at','创建时间戳（秒）'),
  ('column','plugin_configs','updated_at','更新时间戳（秒）'),
  ('column','plugin_configs','is_deleted','逻辑删除标记（0/1）');

INSERT OR IGNORE INTO schema_comments (object_type, table_name, column_name, comment)
VALUES
  ('table','notification_channels','', '通知渠道配置表（push_config）'),
  ('column','notification_channels','id','主键'),
  ('column','notification_channels','user_id','所属用户ID'),
  ('column','notification_channels','channel_key','渠道/键名（如 QUARK_SIGN_NOTIFY/DD_BOT_TOKEN 等）'),
  ('column','notification_channels','enabled','是否启用（0/1，用于布尔型开关）'),
  ('column','notification_channels','config_json','参数（JSON，用于 token/secret 等非布尔项）'),
  ('column','notification_channels','created_at','创建时间戳（秒）'),
  ('column','notification_channels','updated_at','更新时间戳（秒）'),
  ('column','notification_channels','is_deleted','逻辑删除标记（0/1）');

INSERT OR IGNORE INTO schema_comments (object_type, table_name, column_name, comment)
VALUES
  ('table','notification_templates','', '通知模板表（可扩展）'),
  ('column','notification_templates','id','主键'),
  ('column','notification_templates','user_id','所属用户ID'),
  ('column','notification_templates','template_key','模板键'),
  ('column','notification_templates','title_template','标题模板'),
  ('column','notification_templates','body_template','正文模板'),
  ('column','notification_templates','channel_key','默认渠道键（可为空）'),
  ('column','notification_templates','created_at','创建时间戳（秒）'),
  ('column','notification_templates','updated_at','更新时间戳（秒）'),
  ('column','notification_templates','is_deleted','逻辑删除标记（0/1）');

INSERT OR IGNORE INTO schema_comments (object_type, table_name, column_name, comment)
VALUES
  ('table','data_sources','', '数据源配置表（source）'),
  ('column','data_sources','id','主键'),
  ('column','data_sources','user_id','所属用户ID'),
  ('column','data_sources','source_key','数据源键（net/cloudsaver/pansou 等）'),
  ('column','data_sources','enabled','是否启用（0/1）'),
  ('column','data_sources','config_json','数据源配置（JSON）'),
  ('column','data_sources','created_at','创建时间戳（秒）'),
  ('column','data_sources','updated_at','更新时间戳（秒）'),
  ('column','data_sources','is_deleted','逻辑删除标记（0/1）');

INSERT OR IGNORE INTO schema_comments (object_type, table_name, column_name, comment)
VALUES
  ('table','sync_task_configs','', '数据同步配置表（sync_tasks）'),
  ('column','sync_task_configs','id','主键'),
  ('column','sync_task_configs','user_id','所属用户ID'),
  ('column','sync_task_configs','task_id','同步任务ID（业务主键）'),
  ('column','sync_task_configs','taskname','任务名称'),
  ('column','sync_task_configs','source_dir','源目录'),
  ('column','sync_task_configs','dest_dir','目标目录'),
  ('column','sync_task_configs','sync_mode','同步模式（incremental/full 等）'),
  ('column','sync_task_configs','match_mode','比对模式（md5/full_name/name_only 等）'),
  ('column','sync_task_configs','cron','CRON 表达式'),
  ('column','sync_task_configs','enabled','是否启用（0/1）'),
  ('column','sync_task_configs','regex_filter','正则过滤'),
  ('column','sync_task_configs','file_type_filter_json','文件类型过滤（JSON 数组）'),
  ('column','sync_task_configs','exclude_empty_dirs','是否排除空目录（0/1）'),
  ('column','sync_task_configs','notify_on_complete','完成通知（0/1）'),
  ('column','sync_task_configs','notify_on_error','错误通知（0/1）'),
  ('column','sync_task_configs','extra_json','扩展字段（JSON）'),
  ('column','sync_task_configs','created_at','创建时间戳（秒）'),
  ('column','sync_task_configs','updated_at','更新时间戳（秒）'),
  ('column','sync_task_configs','is_deleted','逻辑删除标记（0/1）');

INSERT OR IGNORE INTO schema_comments (object_type, table_name, column_name, comment)
VALUES
  ('table','system_settings','', '系统设置表（crontab/多网盘开关/存储模式等）'),
  ('column','system_settings','id','主键'),
  ('column','system_settings','user_id','所属用户ID'),
  ('column','system_settings','key','设置键'),
  ('column','system_settings','value','设置值'),
  ('column','system_settings','created_at','创建时间戳（秒）'),
  ('column','system_settings','updated_at','更新时间戳（秒）'),
  ('column','system_settings','is_deleted','逻辑删除标记（0/1）');

INSERT OR IGNORE INTO schema_comments (object_type, table_name, column_name, comment)
VALUES
  ('table','schema_comments','', '结构注释元数据表'),
  ('column','schema_comments','id','主键'),
  ('column','schema_comments','object_type','对象类型（table/column/index）'),
  ('column','schema_comments','table_name','表名'),
  ('column','schema_comments','column_name','字段名/索引名（表注释为空字符串）'),
  ('column','schema_comments','comment','注释内容'),
  ('column','schema_comments','created_at','创建时间戳（秒）'),
  ('column','schema_comments','updated_at','更新时间戳（秒）'),
  ('column','schema_comments','is_deleted','逻辑删除标记（0/1）');
