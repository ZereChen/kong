local IanusApmHandler = { _VERSION = "0.0.1-SNAPSHOT" }

local function serialize(obj)
    local lua = ""
    local t = type(obj)
    if t == "number" then
        lua = lua .. obj
    elseif t == "boolean" then
        lua = lua .. tostring(obj)
    elseif t == "string" then
        lua = lua .. string.format("%q", obj)
    elseif t == "table" then
        lua = lua .. "{\n"
        for k, v in pairs(obj) do
            lua = lua .. "[" .. serialize(k) .. "]=" .. serialize(v) .. ",\n"
        end
        local metatable = getmetatable(obj)
        if metatable ~= nil and type(metatable.__index) == "table" then
            for k, v in pairs(metatable.__index) do
                lua = lua .. "[" .. serialize(k) .. "]=" .. serialize(v) .. ",\n"
            end
        end
        lua = lua .. "}"
    elseif t == "nil" then
        return nil
    else
        error("can not serialize a " .. t .. " type.")
    end
    return lua
end

local cjson = require("cjson.safe")
local producer = require "resty.kafka.producer"
--local hash = require "kong.tools.hash"
--local ip_client = require "kong.tools.iputils"
IanusApmHandler.PRIORITY = 50

--数据采集阈值限制，如果lua采集超过阈值，则不采集
local DEFAULT_THRESHOLD = 100000
-- kafka分区数
local PARTITION_NUM = 3
-- kafka主题名称
local TOPIC = 'yanxuan-ianus-apm-metric'
-- 轮询器共享变量KEY值
local POLLING_KEY = "POLLING_KEY"
-- kafka集群(定义kafka broker地址，ip需要和kafka的host.name配置一致)
local function partitioner(key, num, correlation_id)
  return tonumber(key)
end

local BROKER_LIST = { { host = "172.31.16.170", port = 30511 }, { host = "172.31.16.168", port = 31177 }, { host = "172.31.16.171", port = 30977 } }

local CONNECT_PARAMS = { producer_type = "async", socket_timeout = 30000, flush_time = 10000, request_timeout = 20000, partitioner = partitioner }

--local i --可共享
local bp

local function get_header(key, default)
  local value = ngx.req.get_headers()[key]
  if value == nil then
    return default
  else
    return value
  end
end

local function get_append_msg(key, default)
  if ngx.ctx.append_msg ~= nil then
    local value = ngx.ctx.append_msg[key]
    if value then
      return default
    else
      return value
    end
  else
    return default
  end

end

function IanusApmHandler:new()
  --IanusApmHandler.super.new(self, "ianus-apm")
  ngx.log(ngx.ERR,  "[czl] IanusApmHandler new()")
  if bp == nil then
    bp = producer:new(BROKER_LIST, CONNECT_PARAMS)
  end
end

function IanusApmHandler:init_worker()
  --IanusApmHandler.super.init_worker(self)
  ngx.log(ngx.INFO, "[czl] IanusApmHandler init_worker()")
  producer:start_job(CONNECT_PARAMS,bp)
  --if bp == nil then
  --  bp = producer:new(BROKER_LIST, CONNECT_PARAMS)
  --end
end

-- 采集 ianus-caesar 插件赋值的值
function IanusApmHandler:access(conf)
  --IanusApmHandler.super.access(self)
  conf={
    sample = 100
  }

  -- 并发控制
  local metric_data = {}
  if conf.sample == 0 or (conf.sample ~= 100 and math.random(0, 100) > conf.sample) then
    metric_data["isSample"] = false
  else
    metric_data["isSample"] = true
  end

  if ngx.var.host ~= nil and metric_data["isSample"] then
    --开始采集
    metric_data["name"] = "_URL"
    metric_data["time"] = ngx.time() * 1000 -- os.time() * 1000
    metric_data["mtype"] = "T"
    metric_data["category"] = "BE"
    -- meta指标
    metric_data["meta"] = {}
    metric_data["meta"]["application"] = "yanxuan-ianus"
    --HOST_IP_PATTERN = Pattern.compile("^\\{hostIp=(\\d{1,3}.\\d{1,3}.\\d{1,3}.\\d{1,3})");
    metric_data["meta"]["agentId"] = get_header("X-Real-IP", "nil") .. "yanxuan-ianus" --?

    -- transactionId=tid 10.200.179.103(yanxuan-ianus)unSample^1605492372379^473537556
    metric_data["meta"]["transactionId"] = get_header("YX-TransactionID", nil)
    -- ntesTraceId 1#yanxuan-itemcenter-newadmin#1605492352070#IzqsaL1d
    metric_data["meta"]["ntesTraceId"] = get_append_msg("ntesTraceId", nil)
    metric_data["meta"]["spanId"] = get_append_msg("spanId", nil)

    -- tags指标
    metric_data["tags"] = {}
    metric_data["tags"]["path"] = ngx.var.uri --不带参数


    -- fields指标
    metric_data["fields"] = {}
    metric_data["fields"]["host"] = ngx.var.host
    local method = ngx.req.get_method()
    metric_data["fields"]["method"] = method
    metric_data["fields"]["params"] = nil
    if method == "GET" then
      if ngx.var.is_args == "?" then
        metric_data["fields"]["params"] = ngx.var.args
      end
    elseif method == "POST" then
      ngx.req.read_body()
      metric_data["fields"]["params"] = ngx.req.get_post_args()
    end
    metric_data["fields"]["clientIp"] = ngx.var.remote_addr
    --metric_data["fields"]["clientIp"] = ip_client.get_remote_ip()
  end

  ngx.ctx.metric_data = metric_data
end
function IanusApmHandler:header_filter()
  --IanusApmHandler.super.header_filter(self)
  local metric_data = ngx.ctx.metric_data
  if metric_data ~= nil and metric_data["isSample"] then
    metric_data["fields"]["statusCode"] = ngx.status
    metric_data["value"] = 2 --耗时
    --metric_data["fields"]["exceptionMsg"] = ""
    --metric_data["fields"]["err"] = "0"
    --metric_data["tags"]["level"] = "100ms"
    --metric_data["tags"]["exception"] = ""
    ngx.ctx.metric_data = metric_data
  end
end
function gen_hash_code(input)
  input = tostring(input);
  local h = 0
  local len = string.len(input)
  local max = 2147483647
  local min = -2147483648
  local cycle = 4294967296

  for i = 1, len do
    h = 31 * h + string.byte(string.sub(input, i, i));
    while h > max do
      h = h - cycle
    end
    while h < min do
      h = h + cycle
    end
  end
  return h
end

function IanusApmHandler:log(conf)
  --IanusApmHandler.super.log(self)

  if bp == nil then
      ngx.log(ngx.INFO, "[IanusApmHandler] log() kafka producer does not exist now.")
      return
  end

  local metric_data = ngx.ctx.metric_data
  if metric_data ~= nil and metric_data["isSample"] == false then
    return
  end

  local metric_data_json = cjson.encode(metric_data)
  ngx.log(ngx.ERR, "[czl] IanusApmHandler:log()，metric_data_json" .. metric_data_json)

  local hash_value = gen_hash_code(metric_data["meta"]["transactionId"])
  --local hash_value = hash.gen_hash_code(metric_data["meta"]["transactionId"])
  if not hash_value then
    return
  end
  ngx.log(ngx.ERR,  "[czl] IanusApmHandler:log()，metric_data=" .. serialize(metric_data) .. " hash_value=" .. hash_value % PARTITION_NUM)
  local offset, err = bp:send(TOPIC, tostring(hash_value % PARTITION_NUM), metric_data_json)

  if not offset then
    ngx.log(ngx.ERR, "[IanusApmHandler] log() kafka producer send err:", err)
    return
  end
end
return IanusApmHandler
