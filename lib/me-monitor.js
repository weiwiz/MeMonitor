/**
 * Created by jacky on 2017/2/4.
 */
'use strict';
var util = require('util');
var uuid = require('node-uuid');
var logger = require('./mlogger/mlogger');
var async = require('async');
var VirtualDevice = require('./virtual-device').VirtualDevice;
var OPERATION_SCHEMAS = {
  "onMessage": {
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
  }
};

function Monitor(conx, uuid, token, configurator) {
  this.subscribed = [];
  this.intervalId = null;
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
  self.intervalId = setInterval(subscribe, 10 * 1000, self.conx);
};
/*over write( VirtualDevice )
 * */
Monitor.prototype.onMessage = function (message) {
  var self = this;
  logger.debug(message);
  self.messageValidate(message, OPERATION_SCHEMAS.onMessage, function (error) {
    if (!error && message.topic === "device-status") {
      var serviceName = null;
      var services = self.configurator.getConf("services");
      for (var item in  services) {
        logger.debug("service[" + item + "]");
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
  })
};

module.exports = {
  Service: Monitor,
  OperationSchemas: OPERATION_SCHEMAS
};