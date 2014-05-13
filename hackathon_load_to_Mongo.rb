#!/usr/local/bin/ruby

#biggie_home = ENV['BIGGIE_HOME']
biggie_home = '/opt/hadoop/biggie'
lib_path = biggie_home + '/app/lib'
$LOAD_PATH << lib_path

#require 'job_init.rb' # init globals
require 'utilities.rb'  ## xml config parsing
require 'fileutils'
require 'active_support/all'
require 'bson'   ## remove after mongo logging is moved to rest service
require 'mongo'  ## remove after mongo logging is moved to rest service

data_file_dir = '/opt/hadoop/test/vnagtode/EDWBigData/test'

def connect_logger()
		#biggie_home = ENV['BIGGIE_HOME']
		biggie_home = '/opt/hadoop/biggie'
		include Mongo
		mongo_control_conf = Utilities.read_conf(biggie_home + '/config/database/mongo_control_config.xml')
		connection = MongoClient.new(mongo_control_conf['dbserver'],27017).db("hackathonpi")
		#auth = connection.authenticate(mongo_control_conf['user'], mongo_control_conf['pass'])
		collection = connection.collection('hot_listings')
		return collection
end

listing_entry = {}
File.open('/opt/hadoop/test/vnagtode/EDWBigData/test/Hackathonpi_SJ_7days_with_leads.csv').each do |record1|
   record = record1.strip
   field_array = record.split(",")
      #listing_entry["_id"] = Time.now.to_i
      listing_entry["listingId"] = field_array[0]
      listing_entry["masterPropertyId"] = field_array[1]
      listing_entry["city"] = field_array[2]
      listing_entry["state"] = field_array[3]
      listing_entry["zip"] = field_array[4]
      #listing_entry["leads_count"] = field_array[5]
      listing_entry["leads"] = {"count" => field_array[5]}
      listing_entry["social"] = {"count" => field_array[6]}
      listing_entry["views"] = {"count" => field_array[7]}
      puts listing_entry
      col = connect_logger
      col.insert(listing_entry)
      listing_entry ={}
end
