/**
 * Created by jacky on 2017/2/4.
 */
'use strict';
var _ = require('lodash');
var util = require('util');
var uuid = require('node-uuid');
var logger = require('./mlogger/mlogger');
var async = require('async');
var VirtualDevice = require('./virtual-device').VirtualDevice;
var MESSAGE_SCHEMAS = {
  "RPC_MESSAGE": {
    "type": "object",
    "properties": {
      "devices": {
        "type": [
          "string",
          "array"
        ]
      },
      "topic": {
        "type": "string",
        "enum": ["RPC_CALL", "RPC_BACK"]
      },
      "fromUuid": {
        "type": "string"
      },
      "callbackId": {
        "type": "string"
      },
      "payload": {
        "type": "object"
      }
    },
    "required": ["devices", "topic", "fromUuid", "payload"]
  },
  "RPC_CALL": {
    "type": "object",
    "properties": {
      "cmdName": {"type": "string"},
      "cmdCode": {"type": "string"},
      "parameters": {
        "type": [
          "object",
          "array",
          "number",
          "boolean",
          "string",
          "null"
        ]
      }
    },
    "required": ["cmdName", "cmdCode", "parameters"]
  },
  "RPC_BACK": {
    "type": "object",
    "properties": {
      "retCode": {"type": "number"},
      "description": {"type": "string"},
      "data": {
        "type": [
          "object",
          "array",
          "number",
          "boolean",
          "string",
          "null"
        ]
      }
    },
    "required": ["retCode", "description", "data"]
  },
};
var OPERATION_SCHEMAS = {
  "onConfig": {
    "type": "object",
    "properties": {
      "devices": {
        "type": "string"
      },
      "topic": {
        "type": "string"
      },
      "payload": {
        "type": "object",
        "properties": {
          "online": {
            "type": "boolean"
          }
        },
        "required": ["online"]
      },
      "fromUuid": {
        "type": "string"
      }
    },
    "required": ["devices", "topic", "payload", "fromUuid"]
  },
  "getServiceStatus": {
    "type": "array",
    "items": {
      "type": "string"
    }
  }
};

function Monitor(conx, uuid, token, configurator) {
  this.subscribed = [];
  this.serviceStatus = {};
  this.intervalId = null;
  this.serviceNameMap = {};
  this.watchIntervalId = null;
  this.watch = function (self) {
    var services = self.configurator.getConf("services");
    for (var item in  services) {
      var serviceName = item;
      if (util.isNullOrUndefined(self.serviceStatus[serviceName])) {
        self.serviceStatus[serviceName] = [];
      }
      var serviceAry = services[serviceName];
      if (util.isArray(serviceAry)) {
        _.forEach(serviceAry, function (service) {
          if (self.deviceUuid === service.uuid) {
            return;
          }
          self.serviceNameMap[service.uuid] = serviceName;
          async.waterfall([
            function (innerCallback) {
              var requestMsg = {
                devices: service.uuid,
                payload: {
                  cmdName: "status",
                  cmdCode: "0009",
                  parameters: null
                }
              };
              self.message(requestMsg, function (response) {
                if (response.retCode !== 200) {
                  innerCallback({errorId: response.retCode, errorMsg: response.description});
                  return;
                }
                innerCallback(null, response.data);
              });
            }
          ], function (error, result) {
            var serName = self.serviceNameMap[service.uuid];
            if (error) {
              //请求超时
              if (error.errorId === 200003) {
                //查找是否存在服务状态记录
                var serStatus = _.find(self.serviceStatus[serName], function (serviceInfo) {
                  return serviceInfo.uuid === service.uuid;
                });
                //如果不存在则添加服务状态记录
                if (util.isNullOrUndefined(serStatus)) {
                  self.serviceStatus[serName].push({
                    uuid: service.uuid,
                    online: service.online,
                    status: null,
                    timeOut: 1
                  })
                }
                else {
                  //如果连续3次超时，则认为改服务掉线
                  if (++serStatus.timeOut > 3 && "true" === service.online) {
                    serStatus.online = "false";
                    var zkPath1 = "/system/services/" + serName + "/cluster/" + service.uuid + "/online";
                    self.configurator.setConf(zkPath1, "false", function (error) {
                      if (error) {
                        logger.error(error.errorId, error.errorMsg);
                      }
                    });
                  }
                }
              }
              else {
                logger.error(error.errorId, error.errorMsg);
              }
            }
            else {
              //如果之前状态是离线，则恢复服务状态为上线
              if ("false" === service.online) {
                var zkPath = "/system/services/" + serName + "/cluster/" + service.uuid + "/online";
                self.configurator.setConf(zkPath, "true", function (error) {
                  if (error) {
                    logger.error(error.errorId, error.errorMsg);
                  }
                });
              }
              //查找是否存在服务状态记录
              var serviceStatus = _.find(self.serviceStatus[serName], function (serviceInfo) {
                return serviceInfo.uuid === service.uuid;
              });
              //如果不存在则添加服务状态记录
              if (util.isNullOrUndefined(serviceStatus)) {
                self.serviceStatus[serName].push({
                  uuid: service.uuid,
                  online: "true",
                  status: result,
                  timeOut: 0
                })
              }
              else {
                serviceStatus.online = "true";
                serviceStatus.status = result;
                serviceStatus.timeOut = 0;  //重置超时次数
              }
            }
          });
        })
      }
    }
    logger.debug(JSON.stringify(self.serviceStatus, null, 2));
  };
  VirtualDevice.call(this, conx, uuid, token, configurator);
}

util.inherits(Monitor, VirtualDevice);

/**
 * 初始化
 * */
Monitor.prototype.init = function () {
  var self = this;
  var subscribe = function (conx) {
    var services = self.configurator.getConf("services");
    for (var item in  services) {
      var service = services[item];
      if (util.isArray(service)) {
        for (var i = 0, len = service.length; i < len; ++i) {
          var found = false;
          for (var j = 0; j < self.subscribed.length; ++j) {
            if (self.subscribed[j] === service[i].uuid) {
              found = true;
              break;
            }
          }
          if (!found) {
            conx.subscribe(service[i].uuid);
            self.subscribed.push(service[i].uuid);
            logger.debug("service[" + item + "]:" + service[i].uuid + "");
          }
        }
      }
    }
  };
  subscribe(self.conx);
  self.intervalId = setInterval(subscribe, 60 * 1000, self.conx);
  self.watchIntervalId = setInterval(self.watch, 60 * 1000, self);
};
/*over write( VirtualDevice )
 * */
Monitor.prototype.onMessage = function (message) {
  var self = this;
  self.messageValidate(message, OPERATION_SCHEMAS.onConfig, function (error) {
    if (!error && message.topic === "device-status") {
      var serviceName = null;
      var services = self.configurator.getConf("services");
      for (var item in  services) {
        var service = services[item];
        if (util.isArray(service)) {
          for (var i = 0, len = service.length; i < len; ++i) {
            if (service[i].uuid === message.fromUuid) {
              serviceName = item;
              break;
            }
          }
        }
        if (null !== serviceName) {
          break;
        }
      }
      if (serviceName) {
        var zkPath = "/system/services/" + serviceName + "/cluster/" + message.fromUuid + "/online";
        self.configurator.setConf(zkPath, message.payload.online.toString(), function (error) {
          if (error) {
            logger.error(error.errorId, error.errorMsg);
          }
        });
      }
    }
    else {
      if (self.deviceUuid !== message.fromUuid) {
        self.onRpcMessage(message);
      }
    }
  })
};

Monitor.prototype.onRpcMessage = function (message) {
  var self = this;
  //request message
  var retMsg = {
    devices: [message.fromUuid],
    topic: "RPC_BACK",
    fromUuid: self.deviceUuid,
    callbackId: message.callbackId,
    payload: {
      retCode: 200,
      description: "Success.",
      data: {}
    }
  };
  self.messageValidate(message, MESSAGE_SCHEMAS.RPC_MESSAGE, function (error) {
    if (!util.isNullOrUndefined(error)) {
      retMsg.payload = error;
      logger.error(error.retCode, error.description);
      if (message.topic === "RPC_CALL") {
        self.conx.message(retMsg);
      }
    }
    else if (message.topic === "RPC_BACK") {
      self.messageValidate(message.payload, MESSAGE_SCHEMAS.RPC_BACK, function (error) {
        if (util.isNullOrUndefined(error)) {
          self.emit(message.callbackId, message.payload);
        }
        else {
          logger.error(error.retCode, error.description);
        }
      })
    }
    else {
      self.messageValidate(message.payload, MESSAGE_SCHEMAS.RPC_CALL, function (error) {
        if (!util.isNullOrUndefined(error)) {
          logger.error(error.retCode, error.description);
          retMsg.payload = error;
          self.conx.message(retMsg);
        }
        else {
          var func = self[message.payload.cmdName];
          if (util.isNullOrUndefined(func) || !util.isFunction(func)) {
            var logError = {
              errorId: 200004,
              errorMsg: "method name=" + message.payload.cmdName
            };
            logger.error(200004, " method name=" + message.payload.cmdName);
            retMsg.payload.retCode = logError.errorId;
            retMsg.payload.description = logError.errorMsg;
            self.conx.message(retMsg);
          }
          else {
            self.workStatus.total_msg_in++;
            self.tempData.total_msg_in++;
            var logInfo = "method call:" + message.payload.cmdName + ", "
              + "method params:" + JSON.stringify(message.payload.parameters);
            logger.debug(message);
            var requestTime = Date.now();
            func.call(self, message.payload.parameters, function (result) {
              self.tempData.total_msg_in_time += Date.now() - requestTime;
              if (!util.isNullOrUndefined(retMsg.callbackId)) {
                retMsg.payload.retCode = result.retCode;
                retMsg.payload.description = JSON.stringify(result.description);
                retMsg.payload.data = result.data;
                //发送反馈消息
                self.conx.message(retMsg);
              }
            });
          }
        }
      })
    }
  });
};
/**
 * 微服务工作状态
 * @param {object[]} message:消息体
 * @param {method} peerCallback:远程RPC回调
 * */
Monitor.prototype.getServiceStatus = function (message, peerCallback) {
  var self = this;
  var responseMessage = {retCode: 200, description: "Success.", data: {}};
  if (util.isNullOrUndefined(message)) {
    responseMessage.data = self.serviceStatus;
    peerCallback(responseMessage);
    return;
  }
  self.messageValidate(message, OPERATION_SCHEMAS.getServiceStatus, function (error) {
    if (error) {
      peerCallback(error);
      return;
    }
    _.forEach(message, function (item) {
      responseMessage.data[item] = self.serviceStatus[item];
    });
    logger.debug(responseMessage);
    peerCallback(responseMessage);
  });
};
module.exports = {
  Service: Monitor,
  OperationSchemas: OPERATION_SCHEMAS
};