var aggregates = require('./aggregate_from_hourly');
var _ = require('lodash')

var argv = require('optimist')
    .usage('aggregate hourly CalVAD data to months, days, and weeks, optiomally summing out freeways.\nUsage: $0')
    .default('r','/home/james/repos/jem/carbserver/public')
    .alias('r', 'root')
    .describe('r', 'The root directory holding the "public" tree on the web server.')
    .default('d','data')
    .alias('d', 'directory')
    .describe('d', 'The directory under the root directory where the CalVAD data resides')
  .demand('y')
  .alias('y', 'year')
  .describe('y', 'The year. To do multiple values pass more than one, as in -y 2007 -y 2008')
    .argv
;
var root = argv.root;
var subdir = argv.directory;



aggregates.aggregates({
    'root': root
    ,'subdir':subdir
  ,'years':_.flatten([argv.y])
})(function(){console.log('all done');});
