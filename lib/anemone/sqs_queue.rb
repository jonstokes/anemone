require 'fog'
require 'yaml'
require 'base64'
require 'socket'
require 'digest/md5'

class SqsQueue

  attr_reader :sqs, :queue

  def initialize(opts)
    create_sqs_connection(opts)
    if opts[:name]
      create_queue("#{namespace}-#{opts[:name]}")
    elsif opts[:url]
      @q_url = opts[:url]
    else
      raise "Queue name or url required for SqsQueue!"
    end
    q_url
  end
  
  def enq(p)
    payload = is_a_link?(p) ? p : Base64.encode64(Marshal.dump(p))
    sqs.send_message(q_url, payload)
  end

  def deq
    message = sqs.receive_message(q_url)
    return nil if  message.body.nil? || message.body['Message'].first.nil?
    handle = message.body['Message'].first['ReceiptHandle']
    ser_obj = message.body['Message'].first['Body']
    return nil if ser_obj.nil? || ser_obj.empty?
    sqs.delete_message(q_url, handle)
    return ser_obj if is_a_link?(ser_obj)
    Marshal.load(Base64.decode64(ser_obj))
  end

  def <<(p)
    self.enq(p)
  end

  def size
    sqs.get_queue_attributes(q_url, "ApproximateNumberOfMessages").try(:to_i)
  end

  def length
    self.size
  end

  def pop
    self.deq
  end

  def push(p)
    self.enq(p)
  end

  def q_url
    return @q_url if @q_url
    queue.body['QueueUrl']
  end

  def empty?
    self.size == 0
  end

  #private 

  def is_a_link?(s)
    return false unless s.is_a? String
    (s[0..6] == "http://") || (s[0..7] == "https://")
  end

  def create_queue(name)
    begin
      puts "Creating queue #{name}..."
      @queue = @sqs.create_queue(name)
      puts "Queue created!"
    rescue Exception => e
      raise e
    end
  end

  def delete_queue
    @sqs.delete_queue(q_url)
  end
 
  def create_sqs_connection(opts)
    aws_options = {
      :aws_access_key_id => opts[:aws_access_key_id], 
      :aws_secret_access_key => opts[:aws_secret_access_key]
    }
    begin
      @sqs = Fog::AWS::SQS.new(aws_options)
    rescue Exception => e
      raise e
    end
  end

  def namespace
    Digest::MD5.hexdigest(local_ip)
  end

  def local_ip
    orig, Socket.do_not_reverse_lookup = Socket.do_not_reverse_lookup, true  # turn off reverse DNS resolution temporarily

    UDPSocket.open do |s|
      s.connect '64.233.187.99', 1
      s.addr.last
    end
  ensure
    Socket.do_not_reverse_lookup = orig
  end

end
