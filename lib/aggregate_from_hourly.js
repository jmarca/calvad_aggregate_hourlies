/*global console */
/**
 * this is a library generate other aggregates from the hourly aggregates
 **/


var fs = require('fs');
var http = require('http');
var path = require('path')
var _ = require('lodash')
var dirs = require('makedir');
var async = require('async')

/* use a function for the exact format desired... */

function pad(n){return n<10 ? '0'+n : n}

// utility functions to convert times into aggregate times
// pass a string representation of time in, convert to time, aggregate, spit back out at string

var df = require('date-functions');

var aggs={};
aggs.monthly = function(ts_string){
    var d = new Date(ts_string);
    // monthly means set the day to 1, the time to 00:00:00
    return d.dateFormat("Y-m-01");
};
aggs.daily = function(ts_string){
    var d = new Date(ts_string);
    // daily means just strip the time off
    return d.dateFormat("Y-m-d");
};
aggs.hourly = function(ts_string){
    // faster
    return ts_string;
};
aggs.weekly = function(ts_string){
    var d = new Date(ts_string);
    // weekly requires awesome dataFormat call
    return d.dateFormat("Y \\week W");
};

var areatypes = require('calvad_areas')

var time_aggregation=['hourly'];
var other_aggregations=['monthly','daily','weekly','hourly'];

// must get this from header!
var unmapper = {}

// detector count is really not quite that.  it is detected hours.  So
// if you have a time step of one hour, it is detector count.  if you
// have a time aggregation period of one day, it is detector count
// times 24

// "header":["ts","n","hh","not_hh","o","avg_veh_spd","avg_hh_weight","avg_hh_axles","avg_hh_spd","avg_nh_weight","avg_nh_axles","avg_nh_spd","miles","lane_miles","detector_count","detectors"]

var sum_variables = [
    'n','hh','not_hh'
];
var max_variables = [
    'miles','lane_miles'
];
var avg_hh_vars = [
    'avg_hh_weight','avg_hh_axles','avg_hh_spd'
];
var avg_nhh_vars = [
    'avg_nh_weight','avg_nh_axles','avg_nh_spd'
];

var file_combinations = [];

exports.aggregates = function aggregates(options){

    var root =  options.root;
    var subdir = options.subdir;
    var years = options.years
    var jobs = options.jobs
    console.log('using '+jobs+' jobs')

    if(root === undefined) root =  process.cwd();
    if(subdir === undefined) subdir = 'data';


    function file_worker(task,done){
        async.waterfall([check_dir(task)
                        ,check_file
                        ,aggregate
                        ]
                       ,done)
    }

    function check_dir(task){
        return function(callback){

            var p = path.dirname(task.aggfile)
            dirs.makedir(p
                        ,function(err){
                             callback(null,task)
                         })
            return null
        }
    }

    /**
     * check_file(task,callback)
     *
     * check if a file exists
     *
     * task is a hash, must hold an element 'file' containing the
     * filename
     *
     * callback is a callback, which expects (error,task)
     *
     * returns (null,task) if the file is found,
     * returns (task) (an error condition) if the file is not found
     *
     * The idea is to use it in async.waterfall, so that the waterfall
     * is aborted if the file is not there, and continues if it does
     */
    function check_file(task,callback){
        fs.stat(task.aggfile,function(err,stats){
            if(err){
                //console.log('oh goody, not there yet')
                callback(null,task)
            }else{
                //console.log('already have '+task.aggfile);
                callback(task)
            }
        })
        return null
    }


    function checkfs (queue){
        // populate the file combinations array
        // url and file pattern is
        // /data/{area}/{timeagg}/{year}/{filename}

        _.each(years,function(year){
            _.each( areatypes , function(files,area){
                async.each( files, function(file,cb){
                    var split_file = file.split('.')
                    // check that the hourly file exists, then push
                    // the dependent files

                    var hourlyfile = [root,subdir,area,'hourly',year,file].join('/');

                    fs.stat(hourlyfile,function(err,stats){
                        if(err){
                            //console.log('no source hourly file')
                            return cb()
                        }else{
                            //console.log('have source hourly file, push dependent jobs')
                            _.each(other_aggregations,function(agg){
                                _.each([false,true],function(fwy){
                                    var aggpath = [root,subdir,area,agg,year].join('/');
                                    var aggfile = [aggpath,file].join('/');
                                    if(fwy)
                                        aggfile = [aggpath,[split_file[0],'all',split_file[1]].join('.')].join('/');
                                    queue.push({'aggpath':aggpath
                                               ,'hourlyfile':hourlyfile
                                               ,'agg':agg
                                               ,'aggfile':aggfile})
                                });
                            });
                            console.log('hourlyfile: '+hourlyfile+', current queue: '+queue.length()+' jobs')
                            return cb()
                        }
                    },function(e){
                          console.log(queue.length()+' jobs')
                          return null
                      })
                })
            });
            //throw new Error('croak')
            return null
        })
    }

    return function aggregates(next){


        // create a queue object with concurrency 10

        var file_q = async.queue(file_worker, jobs)

        // assign a callback
        file_q.drain = function() {
            console.log('all items have been processed');
            if(next){
                next();
            }
            return null
        }
        checkfs(file_q);
    }
}

/**
 *  aggregate
 *  aggregate the file found at hourlyfile up to agg level and save to
 *  aggpath
 *
 */
function aggregate(options,cb){
    // load the file at hourlyfile, aggregate, and save to aggpath
    // set up the aggregator
    //console.log(options)
    fs.readFile(options.hourlyfile,{encoding:'utf8'}, aggregator(options.agg,options.aggfile,cb));
}


function aggregator(agg,aggpath,cb){
    // finally do the work!

    // I want to do once for freeways, once without (summing up over all freeways)
    // defined by the aggpath filename.  If it says "_all.json" then sum out the freeways

    var path_split = aggpath.split('/');
    var file_split = _.last(path_split).split('.');
    var fwyagg = false;
    if(file_split.length == 3){
        fwyagg=true;
    }
    var hrly_re = /hourly/;
    var timeagg = aggs[agg];
    return function(err,text){
        if(err){
            console.log('cannot read hourly data??' + JSON.stringify(err));
            return cb(err)
        }
        console.log('working on '+agg + ' '+aggpath+ ' freeway aggregate = '+fwyagg);
        // convert data into the JSON object that it really is
        var hourly_data = JSON.parse(text);
        // there is header, and text, and some other stuff I will keep as is.
        var aggoutput = {features:[]};
        _.each(hourly_data,function(value,key){
            if(key !== 'features'){
                if(/hourly/.test(value)){
                    value = value.replace('hourly',agg);
                }
                aggoutput[key] = value;
            }else{
                _.each(value,function(item){
                    var newitem = {'properties':{}};
                    _.each(item,function(value,key){
                        if( key == 'header'){
                            newitem.header=[]
                            // set up the unmapper
                            _.forEach(value
                                     ,function(key,index){
                                          unmapper[key]=index;
                                          if(key=='freeway' && fwyagg){
                                              // skip it in header
                                          }else{
                                              newitem.header.push(key)
                                          }
                                          return null
                                      });
                        }else{
                            if(key != 'properties'){
                                // swap hourly for current aggregation level
                                if(_.isString(value)){
                                    value = value.replace('hourly',agg);
                                    //console.log({key:value})
                                }
                                newitem[key] = value;
                            }else{
                                _.each(item.properties,function(v,k){
                                    if(k != 'data'){
                                        if(_.isString(v)){
                                            v = v.replace('hourly',agg);
                                        }

                                        newitem.properties[k]=v;
                                    }
                                });
                            }
                        }
                    });
                    aggoutput.features.push(newitem);
                });
            }
        });

        if(aggoutput.features[0] && aggoutput.features[0].properties){
            aggoutput.features[0].properties.data = [];

            var grouper = function(){
                // if freeways, use that too
                if(unmapper.freeway !== undefined  ){
                    return function(element,index){
                        return [ timeagg(element[unmapper.ts])
                                 ,element[unmapper.freeway]].join('-') ;
                    };
                }else{
                    return function(element,index){
                        return timeagg(element[unmapper.ts]) ;
                    };
                }
            }();
            // do the heavy lifting with underscore groupBy and reduce
            var intermediate = _.groupBy(hourly_data.features[0].properties.data,grouper)

            // I'm just going to assume this *stays* sorted properly
            // iterate over each group
            var startidx = 1;
            if(unmapper.freeway !== undefined){
                startidx = 2;
            }
            _.each(intermediate,function(value,key){

                // value is a list of elements
                var start = [];
                var detectors = []
                for(var i = 0;i<startidx;i++){
                    start.push(value[0][i]);
                }
                // initialize the rest with zeros
                var n = value.length;
                var j = _.keys(unmapper).length - 2 // not detector_count, detectors
                for(var i = startidx;i<j;i++){
                    start.push(0);
                }

                //console.log(value[0])
                //console.log(start)
                var printix = 0;
                var end = _.reduce(value
                                  ,function(memo, rec){
                                       // simple sums
                                       _.each(sum_variables,function(variable){
                                           memo[unmapper[variable]] += rec[unmapper[variable]];
                                       });
                                       memo[unmapper['o']] += rec[unmapper['o']]/n; //average occupancy...straight average
                                       // weighted sums
                                       _.each(avg_hh_vars,function(variable){
                                           memo[unmapper[variable]] += rec[unmapper[variable]]*rec[unmapper['hh']];
                                       });
                                       _.each(avg_nhh_vars,function(variable){
                                           memo[unmapper[variable]] += rec[unmapper[variable]]*rec[unmapper['not_hh']];
                                       });
                                       memo[unmapper['avg_veh_spd']] += rec[unmapper['avg_veh_spd']]*rec[unmapper['n']];
                                       // max variables
                                       _.each(max_variables,function(variable){
                                           memo[unmapper[variable]] =
                                               memo[unmapper[variable]] > rec[unmapper[variable]] ?
                                               memo[unmapper[variable]] : rec[unmapper[variable]] ;
                                       });
                                       // array concatenation
                                       detectors.push(rec.slice(unmapper['detectors']))
                                       return memo;
                                   }
                                  , start);
                _.each(avg_hh_vars,function(variable){
                    end[unmapper[variable]] /= end[unmapper['hh']];
                });
                //console.log('\n divide by '+end[unmapper['not_hh']]);
                _.each(avg_nhh_vars,function(variable){
                    end[unmapper[variable]] /= end[unmapper['not_hh']];
                });
                //console.log('\n divide by '+end[unmapper['n']]);
                end[unmapper['avg_veh_spd']] /= end[unmapper['n']];
                //console.log(JSON.stringify(end));
                _.each(_.range(startidx,j)
                      ,function(i){
                           if(unmapper.o == i){
                               end[i] =  +(end[i].toFixed(6))
                           }else{
                               end[i] =  +(end[i].toFixed(2))
                           }
                       });
                detectors = _.flatten(detectors)
                detectors = _.unique(detectors)
                detectors.sort()

                aggoutput.features[0].properties.data.push( _.flatten([end,detectors.length,detectors]) );

            });
            // finally, if aggregating over freeways, redo everything to purge freeways
            if(fwyagg){
                // cut and paste programming ahoy!
                delete unmapper.freeway;
                intermediate = _.groupBy(aggoutput.features[0].properties.data,grouper)
                aggoutput.features[0].properties.data = [];


                _.each(intermediate,function(value,key){
                    // value is a list of elements
                    // stash the time stamp in the result
                    var start = []
                    var detectors = []
                    start.push(value[0][0]);
                    // skipping freeway this time
                    start.push('freeway')
                    // initialize the rest with zeros
                    var n = value.length;
                    var j = _.keys(unmapper).length + 1 - 2
                    // deleted freeway, but need to account for it still,
                    // and still the -2 because we're not doing detector_count, detectors
                    for(var i = startidx;i<j;i++){
                        start.push(0);
                    }
                    var printix = 0;
                    var end = _.reduce(value
                                       ,function(memo, rec){
                                           // simple sums
                                           _.each(sum_variables,function(variable){
                                               memo[unmapper[variable]] += rec[unmapper[variable]];
                                           });
                                           memo[unmapper['o']] += rec[unmapper['o']]/n; //average occupancy...straight average
                                           // weighted sums
                                           _.each(avg_hh_vars,function(variable){
                                               memo[unmapper[variable]] += rec[unmapper[variable]]*rec[unmapper['hh']];
                                           });
                                           _.each(avg_nhh_vars,function(variable){
                                               memo[unmapper[variable]] += rec[unmapper[variable]]*rec[unmapper['not_hh']];
                                           });
                                           memo[unmapper['avg_veh_spd']] += rec[unmapper['avg_veh_spd']]*rec[unmapper['n']];
                                           // max variables
                                           // this go around, we sum them up (from each freeway)
                                           _.each(max_variables,function(variable){
                                               memo[unmapper[variable]] =
                                                   memo[unmapper[variable]] += rec[unmapper[variable]];
                                           });
                                            detectors.push(rec.slice(unmapper['detectors']))

                                           return memo;
                                       }
                                       , start);

                    _.each(avg_hh_vars,function(variable){
                        end[unmapper[variable]] /= end[unmapper['hh']];
                    });
                    _.each(avg_nhh_vars,function(variable){
                        end[unmapper[variable]] /= end[unmapper['not_hh']];
                    });
                    end[unmapper['avg_veh_spd']] /= end[unmapper['n']];
                    // again, truncate the decimals
                    _.each(_.range(startidx,j)
                           ,function(i){
                               if(unmapper.o == i){
                                   end[i] =  +(end[i].toFixed(6))
                               }else{
                                   end[i] =  +(end[i].toFixed(2))
                               }
                           });
                    // handle detectors again
                    detectors = _.flatten(detectors)
                    detectors = _.unique(detectors)
                    detectors.sort()

                    // and finally get rid of the freeway column
                    end.splice(1,1);
                    aggoutput.features[0].properties.data.push( _.flatten([end,detectors.length,detectors]) );
                });


            }


        }
        // write aggoutput.data to the desired file
        fs.writeFile(aggpath,JSON.stringify(aggoutput),function(err){
            if(err){
                console.log('error writing '+aggoutput +' '+JSON.stringify(err));
                throw new Error(err);
            }
            // finally, move on to the next
            return cb();
        })
    };

}
