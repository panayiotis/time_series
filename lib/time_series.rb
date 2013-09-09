# coding: utf-8
require 'bundler/setup'

require 'active_support/core_ext/numeric/time.rb'
require 'active_support/core_ext/date/calculations.rb'

#require 'active_support/core_ext/time/calculations.rb'
require_relative "version"
require_relative 'redis_connection'
require_relative 'key'

##
# TimeSeries Class
#
class TimeSeries < RedisConnection
  attr_reader :name, :duration, :resolution
  
##
# Create Timeseries.
#
# @param [String] name The timeseries name.
# @param [Hash] options The options hash.
# @option options [String] :resolution The time resolution: :year, :month, :day, :hour, :minute, :second
# @option options [Integer] :duration Duration is under development. It will allow for example 10, 20 or 30 seconds keys. Now only keys with :minute resolution are available.
  def initialize name, options={}
    super # initialize RedisConnection
    
    @name = name
    @prefix="#{$app_prefix}:ts:#{@name}:"
    
    # parse the resolution option
    if options.has_key? :resolution
      @resolution = options[:resolution]
      resolutions = [:year, :month, :day, :hour, :minute, :second]
      #if the resolution option is invalid raise an exception
      unless resolutions.include?(@resolution) #or @resolution.is_a?(Integer)
        raise ArgumentError.new("resolution can be either :year or :month or :day or :hour or :minute or :second")
      end  
    elsif keys.empty? # default resolution is :minute
          @resolution = :minute
    else # try to guess resolution from existing keys
      max_res = 0
      keys.each do |k|
        res = k.count(':')
        max_res = res if res > max_res
      end
      
      case max_res
        when 8 then @resolution = :second
        when 7 then @resolution = :minute
        when 6 then @resolution = :hour
        when 5 then @resolution = :day
        when 4 then @resolution = :month
        when 3 then @resolution = :year
        else raise ArgumentError.new("Cannot guess resolution from existing keys")
      end #case
    end # if
    
    # define the @duration based on @resolution
    case @resolution
      when :year   then @duration = 12*30*24*3600
      when :month  then @duration = 30*24*3600
      when :day    then @duration = 24*3600
      when :hour   then @duration = 3600
      when :minute then @duration = 60
      when :second then @duration = options[:duration] ||= 20
    end
  end

  # Returns the current time.
  #
  # @return [Time] the current time
  def current_time
    time=Time.at(Time.now.to_i) # this way nsec and usec is 0
    if @resolution == :second
      sec = time.strftime("%S").to_i % @duration
      time = time - sec
    end  
    return time
  end

  # Returns the time of the last key.
  # 
  # @return [Time] the last key's  time
  def last_time
    current_time - @duration
  end

  # Returns the time of the previous key.
  #
  # @return [Time] the previous key's  time  
  def previous_time
    current_time - 2 * @duration
  end

  # Returns the current key
  #
  # @return [String] current key    
  def current_key
    time_to_key current_time, @resolution
  end

  # Returns the last key
  #
  # @return [String] last key    
  def last_key
    time_to_key last_time, @resolution
  end

  # Returns the previous key
  #
  # @return [String] previous key 
  def previous_key
   time_to_key previous_time, @resolution
  end

  # Returns all the keys
  #
  # @return [Array] all the keys in a String Array
  def keys
    return @redis.keys"#{$app_prefix}:ts:#{@name}:*"
  end

  # Deletes all the keys
  #
  # @return Number of keys deleted
  def clear
    i = 0
    keys.each{|k| @redis.del k; i+=1}
    return i 
  end

  # Returns the contents of all the keys
  # TODO Considering to remove this method
  #
  # @return [Hash] contents of all the keys
  def all
    all = Hash.new
    keys.each{ |k| all[k.gsub(/#{@prefix}/,'')]=k.get}
    return all
  end

  # Returns the contents of the last key
  #
  # @return [Hash] contents of the last key
  def last
    last_key.get
  end  

  # Returns the contents of the previous key
  #
  # @return [Hash] contents of the previous key
  def previous
    previous_key.get
  end

  # Push a new Term into the Timeseries
  def push term
    @redis.zincrby current_key, 1, term
  end     

  # Convert a Time object to the respective Key
  # TODO Refactoring
  #
  # @param [Time] time The Time 
  # @option [String] resolution The time resolution: :year, :month, :day, :hour, :minute, :second
  # @return [String] The Key
  def time_to_key time, *resolution
    if resolution.empty?
      return time.strftime("#{@prefix}%Y:%m:%d:%H:%M:%S")
    else
      resolution = resolution.first
      case resolution
        when :year   then return time.strftime("#{@prefix}%Y")
        when :month  then return time.strftime("#{@prefix}%Y:%m")
        when :day    then return time.strftime("#{@prefix}%Y:%m:%d")
        when :hour   then return time.strftime("#{@prefix}%Y:%m:%d:%H")
        when :minute then return time.strftime("#{@prefix}%Y:%m:%d:%H:%M")
        when :second then return time.strftime("#{@prefix}%Y:%m:%d:%H:%M:%S")
        else puts red "wrong resolution in time_to_key"
      end
    end
  end

  # Removes recent keys from a key array
  #
  # @param [Array] array Array of Keys 
  # @param [Integer] Number of seconds that a key is considered recent
  # @return [Array] The new array
  def array_older_than array, time
    array.keep_if { |k| k.time <= Time.now - time }
  end

  # Keys with second resolution
  #
  # @param [Array] time
  # @return [Array] Array with the keys
  def seconds *time
    array = keys.keep_if { |k| k.second? and k.persistant?}
    if time.empty?
      return array
    else
      time = time.first
      return array_older_than array, time
    end
  end

  # Keys with minute resolution
  #
  # @param [Array] time
  # @return [Array] Array with the keys  
  def minutes *time
    array = keys.keep_if { |k| k.minute? and k.persistant?}
    if time.empty?
      return array
    else
      time = time.first
      return array_older_than array, time
    end    
  end

  # Keys with hour resolution
  #
  # @param [Array] time
  # @return [Array] Array with the keys  
  def hours *time
    array = keys.keep_if { |k| k.hour? and k.persistant?}
    if time.empty?
      return array
    else
      time = time.first
      return array_older_than array, time
    end    
  end

  # Keys with day resolution
  #
  # @param [Array] time
  # @return [Array] Array with the keys  
  def days *time
    array = keys.keep_if { |k| k.day? and k.persistant?}
    if time.empty?
      return array
    else
      time = time.first
      return array_older_than array, time
    end    
  end

  # Keys with month resolution
  #
  # @param [Array] time
  # @return [Array] Array with the keys  
  def months *time
    array = keys.keep_if { |k| k.month? and k.persistant?}  
    if time.empty?
      return array
    else
      time = time.first
      return array_older_than array, time
    end  

  end

  # Keys with year resolution
  #
  # @param [Array] time
  # @return [Array] Array with the keys  
  def years *time
    array = keys.keep_if { |k| k.year? and k.persistant?}  
    if time.empty?
      return array
    else
      time = time.first
      return array_older_than array, time
    end  

  end

  # Compress keys
  # Key compression merges the given keys of the same resolution to keys
  # of greater resolution. For example seconds are merged into minutes and
  # days are merged into months.
  # The values of the keys are merged too.
  # After the merge the keys are deleted.
  #
  # @param [Array] keys to be compressed
  # @return [Integer] Number of keys compressed
  def compress keys
    parents = []
    keys.each do |key|
      unless key.recent? or key.year?
        parent = key.parent
        parents << parent unless parents.include? parent
      end
    end
    
    i = 0
    parents.each do |parent|
      unless parent.recent?
        children = parent.persistant_children
        @redis.zunionstore parent, children
        children.each{|k| @redis.del k; i+=1}
      end
    end
    return i 
  end         

  # Remove terms with low scores
  #
  # @param [Array] keys that will be examined
  # @return [Array] Number of keys the operation took place, it doesn't mean that something changed
  def remove_by_score keys, *population
    if population.empty?
      population = 1
    else
      population = population.first
    end
    i = 0    
    keys.each {|k| @redis.zremrangebyscore(k, '-inf', population); i+=1} # TODO What zremrangebyscore returns?
    return i     
  end

  #def term_weights terms, factor
  #  terms.each do |term, value|
  #    terms[term]=value*factor
  #  end
  #  return terms
  #end
  
  def year time;   time_to_key(time, :year).get end 
  def month time;  time_to_key(time, :month).get end 
  def day time;    time_to_key(time, :day).get end 
  def hour time;   time_to_key(time, :hour).get end 
  def minute time; time_to_key(time, :minute).get end 
  def second time; time_to_key(time, :second).get end 
 
end


  
__END__
=begin     
  def get_by_time time
    key = time_to_key time
    if key.exists?
      return get(key) 
    elsif false
      return nil
    end
    #array_to_hash @redis.zrange key, 0, -1, :with_scores => true 
  end
  

  def get_by_resolution time, resolution
    key = time_to_key time, resolution
    if key.exists?
      ;
    elsif key.has_children?
      key.unionize_persistant_children     
    else
      return get_parent time, resolution
      # fix data
      # add ttl
    end
    
    return get(key) 
  end

  def get_parent time, resolution
    case resolution
      when :month  then get_year time
      when :day    then get_month time
      when :hour   then get_day time
      when :minute then get_hour time
      when :second then get_minute time
      else get_year time
    end        
  end
    
  
=end 


