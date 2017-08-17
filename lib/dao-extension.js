
module.exports = function(ctx){

  var extensions = {

    bulk: function (/* args */){
      var args = Array.prototype.slice.call(arguments);

      // context of functions defined in the DataAccessObject (DAO)
      var Model = this;
      var modelName = Model.modelName;

      var connector = Model.getConnector();
      var argsForConnectorFunction = [modelName].concat(args);
      return connector.bulk.apply(ctx, argsForConnectorFunction);
    },

    bulkCreate: function(/* args */){
      var args = Array.prototype.slice.call(arguments);

      // context of functions defined in the DataAccessObject (DAO)
      var Model = this;
      var modelName = Model.modelName;

      var connector = Model.getConnector();
      var argsForConnectorFunction = [modelName].concat(args);
      return connector.bulkCreate.apply(ctx, argsForConnectorFunction);
    },

    bulkUpdate: function(/* args */){
      var args = Array.prototype.slice.call(arguments);

      // context of functions defined in the DataAccessObject (DAO)
      var Model = this;
      var modelName = Model.modelName;

      var connector = Model.getConnector();
      var argsForConnectorFunction = [modelName].concat(args);
      return connector.bulkUpdate.apply(ctx, argsForConnectorFunction);
    },

    bulkDestroy: function(/* args */){
      var args = Array.prototype.slice.call(arguments);

      // context of functions defined in the DataAccessObject (DAO)
      var Model = this;
      var modelName = Model.modelName;

      var connector = Model.getConnector();
      var argsForConnectorFunction = [modelName].concat(args);
      return connector.bulkDestroy.apply(ctx, argsForConnectorFunction);
    }

  };

  return extensions;

};
