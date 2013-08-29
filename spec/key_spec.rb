#!/usr/bin/ruby
# http://mattsears.com/articles/2011/12/10/minitest-quick-reference

require 'bundler/setup'
require 'redis'
require "minitest/autorun"
require 'minitest/pride'
require_relative '../lib/key'

describe "Key String" do

  before do
    $app_prefix="test"
    @redis ||= Redis.new(:path => "/tmp/redis.sock")
    @redis.keys("test:*").each{|k| @redis.del k}
  end

   after do
    @redis.keys("test:*").each{|k| @redis.del k}
  end 

  it "exists?" do
    key = "test:ts:test_ts:2012:11:10:09:08"
    key.exists?.must_equal false
    @redis.zincrby key, 2, "one"
    key.exists?.must_equal true
  end

  it "determines its prefix" do
    key = "test:ts:test_ts:2012:11:10:09:08"
    key.prefix.must_equal "test:ts:test_ts:"
  end  

  it "has_children" do
    key = "test:ts:test_ts:1986:11:04:12:05"
    @redis.zincrby "test:ts:test_ts:1986:11:04:12:05:01", 2, "one"
    key.has_children?.must_equal true
  end

  it "can determine volatile? / persistant? keys" do
    key = "test:ts:test_ts:1986:11:04:12:05"
    @redis.zincrby key, 2, "one"
    key.persistant?.must_equal true
    key.volatile?.must_equal false
    
    @redis.expire key, 10
    key.persistant?.must_equal false
    key.volatile?.must_equal true
    
    "non_existant_key".persistant?.must_equal false
    "non_existant_key".volatile?.must_equal false
  end

  it "has parent" do
    key = "test:ts:test_ts:1986:11:04:12:05"
    @redis.zincrby "test:ts:test_ts:1986:11:04:12", 2, "one"
    key.has_parent?.must_equal true
  end

  it "has non persistant parent" do
    key = "test:ts:test_ts:1986:11:04:12:05" #second resolution
    
    @redis.zincrby "test:ts:test_ts:1986:11:04:12", 2, "one" #create parent
    key.has_persistant_parent?.must_equal true
    @redis.expire "test:ts:test_ts:1986:11:04:12" , 10 #make parent volatile
    key.has_persistant_parent?.must_equal false
    
    "test:ts:test_ts:1986:11:04:12".has_persistant_parent?.must_equal false #key exists, parent no
    "test:ts:test_ts:1986:11".has_persistant_parent?.must_equal false #key and parent do not exist
  end

  it "has volative parent" do
    key = "test:ts:test_ts:1986:11:04:12:05"
    @redis.zincrby "test:ts:test_ts:1986:11:04:12", 2, "one"
    @redis.expire "test:ts:test_ts:1986:11:04:12", 2
    key.has_volatile_parent?.must_equal true
  end
  
  it "has children" do
    key = "test:ts:test_ts:1986:11:04:12:05"
    @redis.zincrby "test:ts:test_ts:1986:11:04:12:05:01", 2, "one"
    @redis.zincrby "test:ts:test_ts:1986:11:04:12:05:02", 2, "one"
    @redis.zincrby "test:ts:test_ts:1986:11:04:12:05:03:00", 2, "one"       
    @redis.zincrby "test:ts:test_ts:1986:11:04:12:06:01", 2, "one" # non children                      
    key.children.size.must_equal 3
  end
  
  it "can separate persistant children" do
    key = "test:ts:test_ts:1986:11:04:12:05"
    @redis.zincrby "test:ts:test_ts:1986:11:04:12:05:01", 2, "one"
    @redis.zincrby "test:ts:test_ts:1986:11:04:12:05:02", 2, "one"
    @redis.zincrby "test:ts:test_ts:1986:11:04:12:05:05", 2, "one"
    @redis.zincrby "test:ts:test_ts:1986:11:04:12:05:06", 2, "one"
    @redis.zincrby "test:ts:test_ts:1986:11:04:12:05:07", 2, "one"
    @redis.zincrby "test:ts:test_ts:1986:11:04:12:05:08", 2, "one"
    @redis.zincrby "test:ts:test_ts:1986:11:04:12:05:09", 2, "one"
    @redis.zincrby "test:ts:test_ts:1986:11:04:12:05:10", 2, "one"                        
    @redis.zincrby "test:ts:test_ts:1986:11:04:12:05:03:00", 2, "one"

    @redis.expire "test:ts:test_ts:1986:11:04:12:05:01", 5   
    @redis.expire "test:ts:test_ts:1986:11:04:12:05:06", 5           
    @redis.expire "test:ts:test_ts:1986:11:04:12:05:08", 5           
    @redis.expire "test:ts:test_ts:1986:11:04:12:05:03:00", 5
                                   
    @redis.zincrby "test:ts:test_ts:1986:11:04:12:06:01", 2, "one" # hobo
    @redis.zincrby "test:ts:test_ts:1986:11:04:12:07", 2, "one" # hobo    
    
    key.children.size.must_equal 9
    key.persistant_children.size.must_equal 5
    
  end  
  
  it "has siblings" do
    key = "test:ts:test_ts:1986:11"
    @redis.zincrby "test:ts:test_ts:1986:12", 2, "one"
    @redis.zincrby "test:ts:test_ts:1986:13", 2, "one"
    @redis.zincrby "test:ts:test_ts:1986:14", 2, "one"    
    @redis.zincrby "test:ts:test_ts:1986:15", 2, "one"       
    @redis.zincrby "test:ts:test_ts:1987:15", 2, "one" # non sibling                 
    key.siblings_keys.size.must_equal 4
  end

  it "can unionize persistant children children" do
    key = "test:ts:test_ts:1986:01"
    @redis.zincrby "test:ts:test_ts:1986:01:10", 1, "one"
    @redis.zincrby "test:ts:test_ts:1986:01:11", 1, "one"
    @redis.zincrby "test:ts:test_ts:1986:01:12", 1, "one"    
    @redis.zincrby "test:ts:test_ts:1986:01:13", 1, "one"
    @redis.zincrby "test:ts:test_ts:1986:01:14:01", 1, "one"            
    @redis.zincrby "test:ts:test_ts:1986:01:14:02", 1, "one"
    @redis.zincrby "test:ts:test_ts:1986:01:10", 2, "two"
    @redis.zincrby "test:ts:test_ts:1986:01:11", 2, "two"
    @redis.zincrby "test:ts:test_ts:1986:01:12", 2, "two"    
    @redis.zincrby "test:ts:test_ts:1986:01:13", 2, "two"
    @redis.zincrby "test:ts:test_ts:1986:01:14:01", 2, "two"            
    @redis.zincrby "test:ts:test_ts:1986:01:14:02", 2, "two"                                    
    key.unionize_persistant_children
    key.exists?.must_equal true
    key.volatile?.must_equal true
  end

  it "detects its resolution" do
    "test:ts:test_ts:1986".resolution.must_equal :year
    "test:ts:test_ts:1986:01".resolution.must_equal :month
    "test:ts:test_ts:1986:01:14".resolution.must_equal :day
    "test:ts:test_ts:1986:01:14:02".resolution.must_equal :hour
    "test:ts:test_ts:1986:01:14:02:58".resolution.must_equal :minute
    "test:ts:test_ts:1986:01:14:02:10:11".resolution.must_equal :second                    
  end
  
    it "detects year, month, ..." do
    "test:ts:test_ts:1986".year.must_equal 1986
    "test:ts:test_ts:1986:01".month.must_equal 1
    "test:ts:test_ts:1986:08:02".day.must_equal 2    
    "test:ts:test_ts:1986:09:03:04".hour.must_equal 4    
    "test:ts:test_ts:1986:10:05:06:07".minute.must_equal 7    
    "test:ts:test_ts:1986:11:08:09:10:11".second.must_equal 11                        
  end

  it "recent" do
    year   = Time.now.year
    month  = Time.now.month
    day    = Time.now.day
    hour   = Time.now.hour
    minute = Time.now.min
    
    #year
    "test:ts:test_ts:#{year+1}".recent?.must_equal false
    "test:ts:test_ts:#{year}".recent?.must_equal true    
    
    #month
    "test:ts:test_ts:#{year+1}:#{month}".recent?.must_equal false    
    "test:ts:test_ts:#{year}:#{month}".recent?.must_equal true        
    
    #second
    "test:ts:test_ts:#{year}:#{month}:#{day}:#{hour}:#{minute+1}:10".recent?.must_equal false    
    "test:ts:test_ts:#{year}:#{month}:#{day}:#{hour}:#{minute}:10".recent?.must_equal true    
  end  


  it "get array and hash from redis" do
    key = "test:ts:test_ts:1986"
    @redis.zincrby "test:ts:test_ts:1986", 1, "one"
    @redis.zincrby "test:ts:test_ts:1986", 2, "two"
    @redis.zincrby "test:ts:test_ts:1986", 3, "three"    
    @redis.zincrby "test:ts:test_ts:1986", 4, "four"
    @redis.zincrby "test:ts:test_ts:1986", 5, "five"            
    @redis.zincrby "test:ts:test_ts:1986", 6, "six"
    @redis.zincrby "test:ts:test_ts:1986", 7, "seven"
    key.get_array.must_be_instance_of Array
    key.get.must_be_instance_of Hash
    h = {"one"=>1, "two"=>2, "three"=>3, "four"=>4, "five"=>5, "six"=>6, "seven"=>7}
    key.get.must_equal h
  end
  
  it "time from key" do
      "test:ts:test_ts:1986".time.must_be_instance_of Time
      "test:ts:test_ts:2000".time.year.must_equal 2000
      "test:ts:test_ts:2001:01".time.month.must_equal 1
      "test:ts:test_ts:2001:01:30:23:00:59".time.sec.must_equal 59
  end



  it "can return a non-existing key based on its children" do
    key = "test:ts:test_ts:2000:12:30" #day
    time = Time.new(2000, 12, 30)
 
    keys = %w(
              test:ts:test_ts:2000:12:30:01 test:ts:test_ts:2000:12:30:02 test:ts:test_ts:2000:12:30:03 
              test:ts:test_ts:2000:12:30:04 test:ts:test_ts:2000:12:30:05 test:ts:test_ts:2000:12:30:06
              
              test:ts:test_ts:2000:12:30:07:01 test:ts:test_ts:2000:12:30:07:02 test:ts:test_ts:2000:12:30:07:03
              
              test:ts:test_ts:2000:12:30:07:05:01 test:ts:test_ts:2000:12:30:07:01:05:02 test:ts:test_ts:2000:12:30:07:01:05:03 
             )
             
    expire = %w( test:ts:test_ts:2000:12:30:01 test:ts:test_ts:2000:12:30:02 test:ts:test_ts:2000:12:30:06
                 test:ts:test_ts:2000:12:30:07:02 test:ts:test_ts:2000:12:30:07:01:05:02 test:ts:test_ts:2000:12:30:07:01:05:03
               )
    keys.each{|k| @redis.zincrby k, 1, "one"}
    expire.each{|k| @redis.expire k, 1}
    
    key.exists?.must_equal false        # key does not exists 
    key.get.must_be_instance_of Hash    # key is created from its children
    key.exists?.must_equal true         # now key exists
    key.volatile?.must_equal true       # key is volatile
    key.get["one"].must_equal 6    
  end

  it "can return a non-existing key without children based on its parent" do
    key = "test:ts:test_ts:2000:12:30" #day
    # 6 sibling days and 6 non children hours
    keys = %w(
              test:ts:test_ts:2000:12:29 test:ts:test_ts:2000:12:28 test:ts:test_ts:2000:12:27 
              test:ts:test_ts:2000:12:26 test:ts:test_ts:2000:12:25 test:ts:test_ts:2000:12:24

              test:ts:test_ts:2000:12:31:01 test:ts:test_ts:2000:12:31:02 test:ts:test_ts:2000:12:31:03 
              test:ts:test_ts:2000:12:31:04 test:ts:test_ts:2000:12:31:05 test:ts:test_ts:2000:12:31:06
             )
             
    expire = %w( test:ts:test_ts:2000:12:29 test:ts:test_ts:2000:12:31:04  )
    keys.each{|k| @redis.zincrby k, 1, "one"}
    expire.each{|k| @redis.expire k, 10}

    #ap key.parent.persistant_children       
    key.exists?.must_equal false
    key.has_children?.must_equal false   # key has no children, only siblings
    key.get.must_be_instance_of Hash     # get will be retrive info from its parent children
    key.exists?.must_equal true
    key.get["one"].must_equal 1
    key.volatile?.must_equal true

  end

end
