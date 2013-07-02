/**
 * test the area aggregates
 */

var _ = require('lodash')
var async = require('async')

var aggregates = require('../lib/aggregate_from_hourly');
var fs = require('fs')
var should = require('should')

describe('aggregate',function(){


    before(function(done){
        aggregates.aggregates({
            'root': 'test'
          ,'subdir': 'data'
          ,'years': [2007]
        })(done)
        return null
    })


    after(function(done){
        // delete the files I make
        var testfiles = ['./test/data/counties/hourly/2007/06019.all.json'
                        ,'./test/data/counties/monthly/2007/06019.json'
                        ,'./test/data/counties/monthly/2007/06019.all.json'
                        ,'./test/data/counties/daily/2007/06019.json'
                        ,'./test/data/counties/daily/2007/06019.all.json'
                        ,'./test/data/counties/weekly/2007/06019.json'
                        ,'./test/data/counties/weekly/2007/06019.all.json'
                        ]
        async.each(testfiles
                  ,function(f,cb){
                       fs.unlink(f,function(e){
                           if(e) console.log('error unlinking '+f)
                           return cb()
                       })
                   }
                  ,done);
        return null
    })
    describe('hourly',function(){
        it('should have all freeways',function(done){
            fs.readFile('./test/data/counties/hourly/2007/06019.all.json'
                       ,{encoding:'utf8'}
                       ,function(err,data){
                            if(err) throw err
                            var agg = JSON.parse(data)
                            should.exist(agg)
                            agg.should.have.property('features')
                            agg.features.should.have.length(1)
                            agg.features[0].should.have.property('header')
                            agg.features[0].header.should.eql(["ts","n","hh","not_hh","o","avg_veh_spd","avg_hh_weight","avg_hh_axles","avg_hh_spd","avg_nh_weight","avg_nh_axles","avg_nh_spd","miles","lane_miles","detector_count","detectors"])
                            agg.features[0].should.have.property('properties')
                            agg.features[0].properties.should.have.property('document','/data6/counties/hourly/2007/Fresno.json')
                            agg.features[0].properties.should.have.property('data')
                            agg.features[0].properties.data.should.be.an.instanceOf(Array)
                            agg.features[0].properties.data.should.have.property('length',8759)
                            agg.features[0].properties.data[0].should.be.an.instanceOf(Array)
                            agg.features[0].properties.data[0].length.should.be.above(16)
                            var detectors = agg.features[0].properties.data[0].slice(15)
                            detectors.sort().should.eql([ '600053', 'wim.10.N' ])
                            return done()
                        })
        })
        return null
    })


    describe('temporal aggregates',function(){
        it('should have each freeways',function(done){
            var times = ['daily','weekly','monthly']
            async.each(times
                      ,function(time,cb){
                           fs.readFile('./test/data/counties/'+time+'/2007/06019.json'
                                      ,{encoding:'utf8'}
                                      ,function(err,data){
                                           if(err) throw err
                                           var agg = JSON.parse(data)
                                           should.exist(agg)
                                           return cb()
                                       })
                       }
                      ,done);
            return null

        })
        it('should have aggregated all freeways',function(done){
            var times = ['daily','weekly','monthly']
            async.each(times
                      ,function(time,cb){
                           var file = './test/data/counties/'+time+'/2007/06019.all.json'
                           console.log('trying to read file '+file)
                           fs.readFile(file
                                      ,{encoding:'utf8'}
                                      ,function(err,data){
                                           if(err) throw err
                                           console.log(file + ' ' +data.length);
                                           var agg = JSON.parse(data)
                                           should.exist(agg)
                                           return cb()
                                       })
                       }
                      ,done);
            return null

        })

        return null
    })
})
