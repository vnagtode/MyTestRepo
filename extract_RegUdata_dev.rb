#!/usr/local/bin/ruby
require 'fileutils'
require 'xmlsimple'	## Ruby Gen for parsing XML file
require 'date'

#put "Starting the process" + Date.today

start_logdate = Date.parse('08-01-2014')
end_logdate = Date.parse('09-01-2014')

puts start_logdate
puts end_logdate

puts "--------------------------"
start_logdate.step(end_logdate).select {|d|
		puts date_to_extract = d.strftime("%Y%m%d") 

		exe_status = `hive -S -hiveconf LOGDATE=#{date_to_extract} -f extract_RegUdata_dev.sql`
                puts "Failed to extract data for #{date_to_extract}" if $?.to_i > 0
        sleep 3        

		}
puts "Finished extracting the data"		

