#!/usr/local/bin/ruby
require 'fileutils'
require 'xmlsimple'	## Ruby Gem for parsing XML file
require 'date'

#put "Starting the process" + Date.today

start_logdate = Date.parse('28-01-2014')
end_logdate = Date.parse('28-01-2014')

puts start_logdate
puts end_logdate

puts "--------------------------"
start_logdate.step(end_logdate).select {|d|
		puts date_to_extract = d.strftime("%Y%m%d") 

`hadoop distcp2 hdfs://paz02sql2700.corp.homestore.net:8020/apps/hive/warehouse/bi.db/instrumentation/logdate=#{date_to_extract}  hdfs://daz02sql2700.corp.homestore.net:8020/apps/hive/warehouse/bi.db/instrumentation/logdate=#{date_to_extract} >> ./log/outoftime_distcp/logdate=#{date_to_extract}.log 2>&1`
#`hadoop distcp2 hdfs://paz02sql2700.corp.homestore.net:8020/apps/hive/warehouse/bi.db/instrumentation/logdate=#{date_to_extract}  hdfs://daz02sql2700.corp.homestore.net:8020/tmp/validate_reco/outoftime/logdate=#{date_to_extract} > ./log/outoftime_distcp/logdate=#{date_to_extract}.log 2>&1`

                puts "Failed to extract data for #{date_to_extract}" if $?.to_i > 0
        sleep 300        

		}
puts "Finished extracting the data"		
#MSCK repair table instrumentation;
