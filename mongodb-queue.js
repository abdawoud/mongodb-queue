/**
 *
 * mongodb-queue.js - Use your existing MongoDB as a local queue.
 *
 * Copyright (c) 2014 Andrew Chilton
 * - http://chilts.org/
 * - andychilton@gmail.com
 *
 * License: http://chilts.mit-license.org/2014/
 *
 **/

var crypto = require('crypto'),
    should = require('should');

var Account = require('../../store/db/Account').Account;
var Task = require('../../store/db/Task').Task;
var constants = require('../../helpers/constants');

// some helper functions
function id() {
    return crypto.randomBytes(16).toString('hex');
};

function now() {
    return (new Date()).toISOString();
};

function pastFromNow(secs) {
    return (new Date(Date.now() - secs * 1000)).toISOString();
};

function nowPlusSecs(secs) {
    return (new Date(Date.now() + secs * 1000)).toISOString();
};

module.exports = function(mongoDbClient, name, opts) {
    return new Queue(mongoDbClient, name, opts);
};

// the Queue object itself
function Queue(mongoDbClient, name, opts) {
    if ( !mongoDbClient ) {
        throw new Error("mongodb-queue: provide a mongodb.MongoClient")
    }
    if ( !name ) {
        throw new Error("mongodb-queue: provide a queue name")
    }
    opts = opts || {};

    this.name = name;
    this.col = mongoDbClient.collection(name);
    this.visibility = opts.visibility || 30;
    this.delay = opts.delay || 0;

    if ( opts.deadQueue ) {
        this.deadQueue = opts.deadQueue;
        this.maxRetries = opts.maxRetries || 5;
    }
}

Queue.prototype.ensureIndexes = function(callback) {
    var self = this;

    self.col.ensureIndex({ visible : 1 }, function(err) {
        if (err) return callback(err);
        self.col.ensureIndex({ ack : 1 }, { unique : true, sparse : true }, function(err) {
            if (err) return callback(err);
            callback();
        });
    });
};

Queue.prototype.add = function(payload, opts, callback) {
    var self = this;
    if ( !callback ) {
        callback = opts;
        opts = {}
    }
    var delay = opts.delay || self.delay;
    var visible = payload['old_visible'];
    if(visible === undefined){
        visible = delay ? nowPlusSecs(delay) : now();
    }
    var msg = {
        visible  : visible,
        payload  : payload
    };
    self.col.insert(msg, function(err, results) {
        try {
            if (err) return callback(err);
            callback(null, '' + results[0]._id);
        } catch (e) {
        }
    })
};


Queue.prototype.addUnique = function(payload, callback) {
    var self = this;

    setTimeout(
        function(){
            self.col.find({'payload.Account_id': payload['Account_id']}, {limit: 1}).toArray(
                function(err, msgs) {
                    try {
                        if(!err){
                            if(msgs.length > 0){
                                callback(null, '' + msgs[0]['_id']);
                            } else {
                                var delay = self.delay;
                                var msg = {
                                    visible  : delay ? nowPlusSecs(delay) : now(),
                                    payload  : payload
                                };
                                self.col.insert(msg, function(err, results) {
                                    if (err) return callback(err);
                                    callback(null, '' + results[0]._id);
                                })
                            }
                        } else {
                            callback(err);
                        }
                    } catch (exception) {
                        callback(exception);
                    }
                }
            );
        }, Math.round(Math.random() * 20000 + 5000)
    )
};

Queue.prototype.changeVisibility = function(ack, delay, callback) {
    var self = this;

    var query = {
        ack : ack
    };

    self.col.findAndRemove(query, function(err, msg, blah) {
        if (err) return callback(err);
        if ( !msg ) {
            return callback(new Error("Queue.del(): Unidentified id : " + id));
        }
        var AccountsQueue = global.AccountsQueue;
        AccountsQueue.add(msg.payload, { delay: delay }, function(err, id) {
            callback(undefined, '' + msg._id);
        });
    });

};

Queue.prototype.update = function(params, callback) {
    var self = this;

    var query = params['condition'];

    var update = {
        $set : params['update']
    };

    var options = params['options'];

    self.col.update(query, update, options, function(err, num) {
        if (err) return callback(err);
        if (!num) return callback();
        callback(null, num);
    });
};

Queue.prototype.get = function(callback, params) {
    var self = this;

    var query = {
        visible : { $lt : now() },
        deleted : { $exists : false }
    };
    var sort = {
        _id : 1
    };
    var update = {
        $set : {
            ack     : id(),
            visible : nowPlusSecs(self.visibility)
        }
    };

    self.col.findAndModify(query, sort, update, { new : true }, function(err, msg) {
        if (err) return callback(err);
        if (!msg) return callback();

        // convert to an external representation
        msg = {
            // convert '_id' to an 'id' string
            id      : '' + msg._id,
            ack     : msg.ack,
            payload : msg.payload,
            tries   : msg.tries
        };

        // if we have a deadQueue, then check the tries, else don't
        if ( self.deadQueue ) {
            // check the tries
            if ( msg.tries > self.maxRetries ) {
                // So:
                // 1) add this message to the deadQueue
                // 2) ack this message from the regular queue
                // 3) call ourself to return a new message (if exists)
                self.deadQueue.add(msg, function(err) {
                    if (err) return callback(err);
                    self.ack(msg.ack, function(err) {
                        if (err) return callback(err);
                        self.get(callback);
                    })
                });
                return;
            }
        }

        callback(null, msg);
    })
};

Queue.prototype.getForInfinity = function(params, callback) {
    var self = this;

    var query = params;

    var sort = {
        _id : 1
    };
    var update = {
        $set : {
            visible : nowPlusSecs(self.visibility)
        }
    };

    self.col.findAndModify(query, sort, update, {  }, function(err, msg) {
        if (err) return callback(err);
        if (!msg) return callback();

        // convert to an external representation
        msg = {
            // convert '_id' to an 'id' string
            id      : '' + msg._id,
            payload : msg.payload
        };

        callback(null, msg);
    })
};

Queue.prototype.requeueTasks = function (condition, callback) {

    var TasksQueue = global.TasksQueue;
    var TasksCanNotBeAdded = global.TasksCanNotBeAdded

    try {
        //remove from ToBeSentQueue and add into TasksQueue
        var self = this;

        var query = condition;

        self.col.findAndRemove(query, function (err, msg) {
            try {
                if (err) {
                    console.log(err.toString());
                    return callback(err);
                }
                if (!msg) return callback();

                if(msg['payload']['TriesCount'] >= constants.MaxTriesForSending){
                    Task.update(
                        {
                            _id: msg['payload']['Task_id']
                        },
                        {
                            $set:{
                                Status: constants.TaskStatus.Fail,
                                Reason: constants.TaskFailReason.MaxSendingTriesExceeded
                            }
                        },
                        function(){
                            callback(null, msg['_id']);
                        }
                    );
                } else {
                    var Account_id = msg['payload']['Account_id'];
                    delete msg['payload']['Account_id'];
                    TasksQueue.add(msg['payload'], function (err, msg) {
                        try {
                            if (err) {
                                console.log(err.toString());
                                TasksCanNotBeAdded.add(msg['payload'], function(){});
                            }
                            Account.update(
                                {
                                    _id: Account_id
                                },
                                {
                                    isBeingUsed: false
                                },
                                function(error, account){
                                    try {
                                        if(error === null){
                                            callback(null, msg['_id']);
                                        } else {
                                            callback(error);
                                        }
                                    } catch (exception) {
                                        callback(exception);
                                    }
                                }
                            );
                        } catch (e) {

                        }
                    });
                }

            } catch (e) {
                callback(e);
            }
        })
    } catch (e) {
        callback(e);
    }
};

Queue.prototype.getStream = function(params, callback) {
    var self = this;

    var query = params['condition'];

    var stream = self.col.find(query).sort({_id: 1}).stream();

    callback(null, stream);
};

Queue.prototype.getWithCondition = function(params, callback) {

    try {

        (params['Account_id'].toString()).should.be.ok;
        (params['limit']).should.be.a.Number;

        var account_id = params['Account_id'];
        var limit = params['limit'];

        var self = this;

        var query = {
            'payload.Account_id': account_id
        };

        var sort = {
            "payload.Campaign_id": 1
        };

        var key = Date.now() + '' + Math.round(Math.random() * 1000000);

        self.col.update(
            query,
            {$set: {key: key}},
            {multi:true},
            function(err, num) {
                try {
                    if(!err){
                        query['key'] = key;
                        self.col.find(query, {sort: sort, limit: limit}).toArray(
                            function(err, msgs) {
                                try {
                                    if(!err){
                                        callback(null, msgs);
                                    } else {
                                        callback(err, []);
                                    }
                                } catch (exception) {
                                    callback(exception, []);
                                }
                            }
                        );
                    } else {
                        callback(err, []);
                    }
                } catch (exception) {
                    callback(exception, []);
                }
            }
        );

    } catch (exception) {
        callback(exception, []);
    }
};

Queue.prototype.getWithPriority = function(params, callback) {

    try {

        (params['Account_id']).should.be.ok;
        (params['Queue_id']).should.be.ok;
        (params['limit']).should.be.a.Number;

        var Account_id = params['Account_id'];
        var Queue_id = params['Queue_id'];
        var limit = params['limit'];
        var skip = params['skip'];

        var ToBeSentQueue = global.ToBeSentQueue;

        var self = this;

        var query = {
            visible : { $lt : now() },
            'payload.Queue_id': Queue_id
        };

        var sort = {
            _id : 1
        };

        self.col.find(query, {sort: sort, limit: limit, skip: skip}).toArray(
            function(err, msgs) {
                //consoe.log(msgs);
                try {
                    if(!err){
                        var counter = 0;
                        var counter_all = 0;
                        var tasks_arr = [];
                        msgs.forEach(
                            function(msg){
                                try {
                                    self.col.findAndRemove({_id: msg._id}, function(err, message) {
                                        try {
                                            if(!err){
                                                if(message != null){
                                                    message['payload']['Account_id'] = Account_id;
                                                    message['payload']['TaskQueue_id'] = message._id;
                                                    var campaign = null;
                                                    ToBeSentQueue.add(message['payload'], function(err, msg){
                                                        if(err === null){
                                                            counter++;
                                                            campaign = JSON.parse(message['payload']['Campaign']);
                                                            var msgs = campaign['Messages'];
                                                            counter_all += msgs.length;

                                                            var task = {
                                                                number: message['payload']["PhoneNumber"],
                                                                msgs: []
                                                            };

                                                            for(var i = 0; i < msgs.length; i++){
                                                                task['msgs'].push(
                                                                    {
                                                                        type: msgs[i]['ID']['Type'],
                                                                        content: msgs[i]['ID']['Body'],
                                                                        task_id: msg,
                                                                        extra: {
                                                                            raw_task_id: message['payload']["Task_id"],
                                                                            number_id: message['payload']["Number_id"],
                                                                            campaign_id: message['payload']["Campaign_id"],
                                                                            queue_id: params['Queue_id'],
                                                                            user_id: message['payload']['User_id'],
                                                                            io_id: params['io_id']
                                                                        }
                                                                    }
                                                                );
                                                            }

                                                            var index = -1;
                                                            for(var i = 0; i < tasks_arr.length; i++){
                                                                if(tasks_arr[i]['number'] === message['payload']["PhoneNumber"]){
                                                                    index = i;
                                                                    break;
                                                                }
                                                            }

                                                            if(index >= 0){
                                                                for(var i = 0; i < task['msgs'].length; i++){
                                                                    tasks_arr[index]['msgs'].push(task['msgs'][i]);
                                                                }
                                                            } else {
                                                                tasks_arr.push(task);
                                                            }

                                                            if(counter_all === msgs.length){
                                                                callback(null, counter, tasks_arr, counter_all);
                                                            }
                                                        } else {
                                                            console.log('message_can_not_be_added:'+message['payload']['Task_id']);
                                                        }
                                                    });
                                                } else {

                                                }
                                            } else {
                                                callback(null, counter, tasks_arr, counter_all);
                                            }
                                        } catch (exception) {
                                            callback(null, counter, tasks_arr, counter_all);
                                        }
                                    });
                                } catch (exception){
                                    callback(null, counter, tasks_arr, counter_all);
                                }
                            }
                        );
                    } else {
                        callback(err);
                    }
                } catch (exception) {
                    callback(exception);
                }
            }
        );

    } catch (exception) {

        callback(exception);

    }

};

Queue.prototype.ping = function(ack, callback) {
    var self = this;

    var query = {
        ack     : ack,
        visible : { $gt : now() },
        deleted : { $exists : false }
    };
    var update = {
        $set : {
            visible : nowPlusSecs(self.visibility)
        }
    };
    self.col.findAndModify(query, undefined, update, { new : true }, function(err, msg, blah) {
        if (err) return callback(err);
        if ( !msg ) {
            return callback(new Error("Queue.ping(): Unidentified ack  : " + ack));
        }
        callback(null, '' + msg._id);
    })
}

Queue.prototype.ack = function(ack, callback) {
    var self = this;

    var query = {
        ack     : ack,
        visible : { $gt : now() },
        deleted : { $exists : false }
    };
    var update = {
        $set : {
            deleted : now()
        }
    };
    self.col.findAndModify(query, undefined, update, { new : true }, function(err, msg, blah) {
        if (err) return callback(err);
        if ( !msg ) {
            return callback(new Error("Queue.ack(): Unidentified ack : " + ack));
        }
        callback(null, '' + msg._id);
    })
};

Queue.prototype.del = function(_id, callback) {
    var self = this;

    self.col.findByIdAndRemove(_id, function(err, msg) {
        if (err) return callback(err);
        if ( !msg ) {
            return callback(new Error("Queue.del(): Unidentified _id : " + _id));
        }
        callback(null, '' + msg._id);
    });
};

Queue.prototype.count = function(condition, callback) {
    var self = this;

    var query = condition;

    self.col.count(query, function(err, count) {
        if (err) return callback(err);
        callback(null, count);
    });
};

Queue.prototype.distinct = function(condition, callback) {
    var self = this;

    var query = condition;

    self.col.distinct(query, function(err, data) {
        if (err) return callback(err);
        callback(null, data);
    });
};

Queue.prototype.delAll = function() {
    var self = this;
    var query = {};
    self.col.remove(query, function(err, msg, blah) {
    });
};

Queue.prototype.delById = function(id, callback) {
    var self = this;

    var query = {
        _id     : id
    };
    self.col.findAndRemove(query, function(err, msg) {
        if (err) return callback(err);
        if ( !msg ) {
            return callback(new Error("Queue.del(): Unidentified id : " + id));
        }
        callback(null, '' + msg._id);
    });

};

Queue.prototype.delByCondition = function(condition, callback) {
    var self = this;

    var query = condition;

    self.col.findAndRemove(query, function(err, msg) {
        if (err) return callback(err);
        if ( !msg ) {
            return callback(new Error("Queue.del(): Unidentified id : " + id));
        }
        callback(null, '' + msg._id);
    });
};

Queue.prototype.total = function(callback) {
    var self = this;

    self.col.count(function(err, count) {
        if (err) return callback(err);
        callback(null, count);
    })
};

Queue.prototype.size = function(callback) {
    var self = this;

    var query = {
        visible : { $lt : now() },
        deleted : { $exists : false }
    };

    self.col.count(query, function(err, count) {
        if (err) return callback(err);
        callback(null, count)
    })
};

Queue.prototype.inFlight = function(callback) {
    var self = this;

    var query = {
        visible : { $gt : now() },
        ack     : { $exists : true },
        deleted : { $exists : false }
    };

    self.col.count(query, function(err, count) {
        if (err) return callback(err);
        callback(null, count)
    })
};

Queue.prototype.done = function(callback) {
    var self = this;

    var query = {
        deleted : { $exists : true }
    };

    self.col.count(query, function(err, count) {
        if (err) return callback(err);
        callback(null, count);
    })
};
