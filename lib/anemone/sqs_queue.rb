require 'fog'
require 'yaml'
require 'base64'
require 'socket'
require 'digest/md5'

class SqsQueue

  attr_reader :sqs, :sqs_queue, :queue

  def initialize(opts)
    check_opts(opts)

    @waiting = []
    @waiting.taint
    self.taint
    @mutex = Mutex.new
    @in_buffer = SizedQueue.new(opts[:buffer_size])
    @out_buffer = SizedQueue.new(opts[:buffer_size])

    create_sqs_connection(opts)
    create_sqs_queue(opts)

    @sqs_tracker = Thread.new { poll_sqs }
  end

  def push(p)
    @mutex.synchronize {
      @in_buffer.push p
      begin
        t = @waiting.shift
        t.wakeup if t
      rescue ThreadError
        retry
      end
    }
  end

  def pop(non_block=false)
    @mutex.synchronize{
      while true
        if @out_buffer.empty?
          raise ThreadError, "queue empty" if non_block
          @waiting.push Thread.current
          @mutex.sleep
        else
          return m
        end
      end
    }
  end

  def length
    sqs.get_queue_attributes(q_url, "ApproximateNumberOfMessages").try(:to_i) || 0
  end

  def empty?
    self.size == 0
  end

  def num_waiting
    @waiting.size
  end

  def clear
    delete_queue
  end

  alias enq push
  alias << push

  alias deq pop
  alias shift pop

  alias size length

  private

  def check_opts(opts)
    raise "Parameter :buffer_size required!" unless opts[:buffer_size]
    raise "Minimun :buffer_size is 5." unless opts[:buffer_size] >= 5
    raise "AWS credentials :aws_access_key_id and :aws_secret_access_key required!" unless opts[:aws_access_key_id] && opts[:aws_secret_access_key]
    raise "Parameter :name required!" unless opts[:name]
  end

  def create_sqs_queue
    if opts[:name]
      create_queue("#{namespace}-#{opts[:name]}")
    elsif opts[:url]
      @q_url = opts[:url]
    else
      raise "Queue name or url required for SqsQueue!"
    end
  end

  def create_queue(name)
    begin
      puts "Creating queue #{name}..."
      @sqs_queue = @sqs.create_queue(name)
      puts "Queue created!"
    rescue Exception => e
      raise e
    end
  end

  def q_url
    return @q_url if @q_url
    queue.body['QueueUrl']
  end

  def send_message_to_queue(p)
    payload = is_a_link?(p) ? p : Base64.encode64(Marshal.dump(p))
    sqs.send_message(q_url, payload)
  end

  def get_message_from_queue
    message = sqs.receive_message(q_url)
    return nil if  message.body.nil? || message.body['Message'].first.nil?
    handle = message.body['Message'].first['ReceiptHandle']
    ser_obj = message.body['Message'].first['Body']
    return nil if ser_obj.nil? || ser_obj.empty?
    sqs.delete_message(q_url, handle)
    return ser_obj if is_a_link?(ser_obj)
    Marshal.load(Base64.decode64(ser_obj))
  end

  def is_a_link?(s)
    return false unless s.is_a? String
    (s[0..6] == "http://") || (s[0..7] == "https://")
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
