# coding: utf-8

require 'bundler/setup'
require 'redis'
require 'hiredis'
require 'date'
#require 'json'
require 'awesome_print'

class Time
#  def self.from_key key
#    key.gsub!(/^[^:]*:[^:]*:[^:]*:/, '')
#    Time.strptime(key, "%Y:%m:%d:%H:%M:%S") # TODO works only with second-resolution
#  end

  def human
    a = (Time.now-self).to_i
    case a
      when 0 then 'just now'
      when 1 then 'a second ago'
      when 2..59 then a.to_s+' seconds ago' 
      when 60..119 then 'a minute ago' #120 = 2 minutes
      when 120..3540 then (a/60).to_i.to_s+' minutes ago'
      when 3541..7100 then 'an hour ago' # 3600 = 1 hour
      when 7101..82800 then ((a+99)/3600).to_i.to_s+' hours ago' 
      when 82801..172000 then 'a day ago' # 86400 = 1 day
      when 172001..518400 then ((a+800)/(60*60*24)).to_i.to_s+' days ago'
      when 518400..1036800 then 'a week ago'
      else ((a+180000)/(60*60*24*7)).to_i.to_s+' weeks ago'
    end
  end
end

class String
  
  def redis
    @redis = $redis ||= Redis.new(:path => "/tmp/redis.sock",:driver => :hiredis)
    return @redis
  end
  
  def prefix
    matchdata = /(^[^:]+:[^:]+:[^:]+:)/.match(self)
    return matchdata[1] unless matchdata.nil?
  end
  
  def exists?
    redis.exists self
  end
  
  #deprecated
  def parent_key
    parent
  end
    
  def parent
    self.gsub(/:[^:]*$/, '')
  end

  def has_persistant_children?
    not persistant_children.empty?
  end

  def has_children?
    has_persistant_children?
  end
  
  #deprecated  
  def has_parent?
    parent.exists?
  end

  def volatile?
    redis.ttl(self) >= 0  
  end
  
    def persistant?
    redis.ttl(self)==-1 and exists?  
  end
  
  def ttl
    redis.ttl self  
  end 

  #deprecated
  def has_persistant_parent?
    redis.ttl(parent_key) < 0 and parent.exists?
  end

  #deprecated  
  def has_volatile_parent?
    redis.ttl(parent_key) >= 0
  end
  
  def children
    redis.keys(self+":*")
  end

  def persistant_children
    keys = redis.keys(self+":*")
    keys.delete_if{|k| k.volatile?}
    return keys
  end  
  
  def siblings_keys
    redis.keys(self.gsub(/:[^:]*$/, ':*'))
  end
  

  def unionize_persistant_children
    children = persistant_children
    redis.zunionstore self, children
    
    if recent?
      redis.expire self, 60
    else
      redis.expire self, 600      
    end
    
  end
  
  def resolution
    case self.count(':')
      when 8 then :second
      when 7 then :minute
      when 6 then :hour
      when 5 then :day
      when 4 then :month
      when 3 then :year
    end   
  end
  
  def year?;   resolution == :year end
  def month?;  resolution == :month end
  def day?;    resolution == :day end
  def hour?;   resolution == :hour end
  def minute?; resolution == :minute end
  def second?; resolution == :second end
              
  def year;   /[^:]+:[^:]+:[^:]+:([^:]+)/.match(self)[1].to_i end
  def month;  /[^:]+:[^:]+:[^:]+:[^:]+:([^:]+)/.match(self)[1].to_i end
  def day;    /[^:]+:[^:]+:[^:]+:[^:]+:[^:]+:([^:]+)/.match(self)[1].to_i end
  def hour;   /[^:]+:[^:]+:[^:]+:[^:]+:[^:]+:[^:]+:([^:]+)/.match(self)[1].to_i end    
  def minute; /[^:]+:[^:]+:[^:]+:[^:]+:[^:]+:[^:]+:[^:]+:([^:]+)/.match(self)[1].to_i end    
  def second; /[^:]+:[^:]+:[^:]+:[^:]+:[^:]+:[^:]+:[^:]+:[^:]+:([^:]+)/.match(self)[1].to_i end
  
  def time
    key = self.gsub(/^[^:]*:[^:]*:[^:]*:/, '') #remove prefix
    case resolution
      when :year   then return DateTime.new(year).to_time
      when :month  then return DateTime.new(year, month).to_time
      when :day    then return DateTime.new(year, month, day).to_time
      when :hour   then return DateTime.new(year, month, day, hour, 0,      0,      '+3').to_time
      when :minute then return DateTime.new(year, month, day, hour, minute, 0,      '+3').to_time
      when :second then return DateTime.new(year, month, day, hour, minute, second, '+3').to_time
    end  
  end
  
  def recent?
    case resolution
      when :year   then year == Time.now.year
      when :month  then year == Time.now.year
      when :day    then year == Time.now.year and month == Time.now.month
      when :hour   then year == Time.now.year and month == Time.now.month and day == Time.now.day
      when :minute then year == Time.now.year and month == Time.now.month and day == Time.now.day and hour == Time.now.hour
      when :second then year == Time.now.year and month == Time.now.month and day == Time.now.day and hour == Time.now.hour and minute == Time.now.min
    end  
  end
 
  def get_array
    redis.zrevrange self, 0, -1, :with_scores => true
  end

  def array_to_hash array
    hash = Hash.new
    array.each{|a| hash[a[0]] = a[1].to_f }
    return hash
  end
    
  def get
    if exists?
      ;
    elsif has_children?
      unionize_persistant_children
    else
      if not year?
        parent.get
        # http://rubydoc.info/github/redis/redis-rb/Redis:zunionstore
        p = parent #TODO Test the case when the immediate parent has no persistant children so weights has infinite values
        while !p.has_children?
          p = p.parent
        end
        weights = [ 1.0 / p.persistant_children.size ]      
        redis.zunionstore self, [parent], :weights => weights
        if recent?
          redis.expire self, 60
        else
          redis.expire self, 600      
        end
      else
        return {}
      end
    end
    array_to_hash get_array
  end
end
