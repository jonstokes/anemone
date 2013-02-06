require 'fog'

class SqsQueue

  attr_reader :sqs, :q_url

  def initialize(opts)
    @sqs = Fog::AWS::SQS.new(aws_options)
    if opts[:name]
      @queue = @sqs.create_queue(opts[:name])
      @q_url = @queue.body['QueueUrl']
    elsif opts[:url]
      @q_url = opts[:url]
    else
      raise "Queue name or url required for SqsQueue!"
    end
    q_url
  end
  
  def enq(p)
    payload = Base64.ecnode64(Marshall.dump(p))
    sqs.send_message(q_url, payload)
  end

  def deq
    message = sqs.recieve_message(q_url)
    ser_obj = message.body['Message'].first['Body']
    Basec64.decode64(Marshall.load(ser_obj))
  end

  def <<(p)
    self.enq(p)
  end

  #private 
  def aws_options
    aws_config = YAML.load(File.read(File.join('config', 'aws.yml')))[Rails.env]
    {
      :aws_access_key_id => aws_config['access_key_id'], 
      :aws_secret_access_key => aws_config['secret_access_key']
    }
  end

end
