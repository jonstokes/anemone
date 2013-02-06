require 'fog'
require 'yaml'
require 'base64'
require 'socket'
require 'digest/md5'

class SqsQueue

  attr_reader :sqs, :q_url

  def initialize(opts)
    create_sqs_connection
    if opts[:name]
      create_queue("scoperrific-#{instance_id}-#{opts[:name]}")
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
    message = sqs.recieve_message(q_url)
    ser_obj = message.body['Message'].first['Body']
    return ser_obj if is_a_link?(ser_obj)
    Marshal.load(Base64.decode64(ser_obj))
  end

  def <<(p)
    self.enq(p)
  end

  def is_a_link?(s)
    return false unless s.is_a? String
    (s[0..6] == "http://") || (s[0..7] == "https://")
  end

  #private 
  def aws_options
    aws_config = YAML.load(File.read(File.join('config', 'aws.yml')))[Rails.env]
    {
      :aws_access_key_id => aws_config['access_key_id'], 
      :aws_secret_access_key => aws_config['secret_access_key']
    }
  end

  def create_queue(name)
    begin
      @queue = @sqs.create_queue(name)
    rescue Exception => e
      raise e
    end
    begin
      @q_url = @queue.body['QueueUrl']
    rescue Exception => e
      raise e
    end
  end

  def create_sqs_connection
    begin
      @sqs = Fog::AWS::SQS.new(aws_options)
    rescue Exception => e
      raise e
    end
  end

  def instance_id
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
