#!/usr/bin/env ruby

require 'aws-sdk-sns'
require 'logger'
require 'optparse'
require 'json'
require 'securerandom'

# Parse command line arguments
options = {
  topic_arn: nil,
  message_type: 'json',
  sender_id: "sender-#{SecureRandom.hex(4)}",
  region: 'us-east-1',
  attributes: {}
}

# Initialize empty message data
message_data = {}

# Parse command line options
OptionParser.new do |opts|
  opts.banner = "Usage: #{$PROGRAM_NAME} [options]"
  
  opts.on('-t', '--topic-arn ARN', 'SNS Topic ARN') do |arn|
    options[:topic_arn] = arn
  end
  
  opts.on('-i', '--id IDENTIFIER', 'Sender identifier') do |id|
    options[:sender_id] = id
  end
  
  opts.on('-r', '--region REGION', 'AWS Region (defaults to us-east-1)') do |region|
    options[:region] = region
  end
  
  opts.on('-k', '--key-value KEY=VALUE', 'Add a key-value pair to the message (can be used multiple times)') do |kv|
    key, value = kv.split('=', 2)
    if key && value
      # Try to convert value to numeric or boolean if appropriate
      case value.downcase
      when 'true'
        message_data[key] = true
      when 'false'
        message_data[key] = false
      when /\A-?\d+\z/
        message_data[key] = value.to_i
      when /\A-?\d+\.\d+\z/
        message_data[key] = value.to_f
      else
        message_data[key] = value
      end
    end
  end
  
  opts.on('-m', '--message JSON_STRING', 'Provide entire message as a JSON string') do |json_str|
    begin
      message_data = JSON.parse(json_str)
    rescue JSON::ParserError => e
      puts "Error parsing JSON: #{e.message}"
      exit(1)
    end
  end
  
  opts.on('-f', '--file FILE_PATH', 'Load message from JSON file') do |file_path|
    begin
      message_data = JSON.parse(File.read(file_path))
    rescue JSON::ParserError => e
      puts "Error parsing JSON file: #{e.message}"
      exit(1)
    rescue Errno::ENOENT => e
      puts "Error reading file: #{e.message}"
      exit(1)
    end
  end
  
  opts.on('-a', '--attribute NAME=VALUE:TYPE', 'Add a message attribute (NAME=VALUE:TYPE)') do |attr|
    name, value_type = attr.split('=', 2)
    if name && value_type
      value, type = value_type.split(':', 2)
      if value && type
        options[:attributes][name] = {
          data_type: type.capitalize,
          string_value: value
        }
      end
    end
  end
  
  opts.on('-h', '--help', 'Display this help message') do
    puts opts
    exit
  end
end.parse!

# Validate required parameters
if options[:topic_arn].nil?
  puts "Error: Topic ARN is required. Use --topic-arn option."
  exit(1)
end

# Validate message has content
if message_data.empty?
  puts "Error: Message is empty. Add content with --key-value, --message, or --file options."
  exit(1)
end

# Add timestamp and sender_id to message metadata
message_data = {
  metadata: {
    sender_id: options[:sender_id],
    timestamp: Time.now.utc.iso8601,
    message_id: SecureRandom.uuid
  },
  data: message_data
} unless message_data.key?('metadata')

# Custom log formatter that includes sender ID
class SenderFormatter < Logger::Formatter
  def initialize(sender_id)
    @sender_id = sender_id
  end
  
  def call(severity, time, progname, msg)
    formatted_time = time.strftime("%Y-%m-%d %H:%M:%S.%L")
    "[#{formatted_time}] [#{severity}] [Sender: #{@sender_id}] #{msg}\n"
  end
end

# Initialize logger with custom formatter
logger = Logger.new(STDOUT)
logger.level = Logger::INFO
logger.formatter = SenderFormatter.new(options[:sender_id])

# Log configuration details
logger.info("Starting SNS message sender")
logger.info("Topic ARN: #{options[:topic_arn]}")
logger.info("Region: #{options[:region]}")
logger.info("Sender ID: #{options[:sender_id]}")

begin
  # Initialize AWS SNS client
  sns = Aws::SNS::Client.new(region: options[:region])
  logger.info("Connected to SNS in region #{options[:region]}")
  
  # Prepare the message
  message_json = JSON.generate(message_data)
  logger.info("Prepared JSON message (#{message_json.bytesize} bytes)")
  
  # Show message preview
  if message_json.length > 200
    logger.info("Message preview: #{message_json[0..200]}...")
  else
    logger.info("Message: #{message_json}")
  end
  
  # Prepare message attributes
  message_attributes = {}
  options[:attributes].each do |name, attr|
    message_attributes[name] = attr
  end
  
  # Always add a content-type attribute
  message_attributes['content-type'] = {
    data_type: 'String',
    string_value: 'application/json'
  }
  
  # Send the message to SNS
  response = sns.publish(
    topic_arn: options[:topic_arn],
    message: message_json,
    message_attributes: message_attributes
  )
  
  # Log success
  logger.info("Message published successfully")
  logger.info("Message ID: #{response.message_id}")
  
rescue Aws::SNS::Errors::ServiceError => e
  logger.error("SNS service error: #{e.message}")
  exit(1)
rescue StandardError => e
  logger.error("Unexpected error: #{e.message}")
  logger.error(e.backtrace.join("\n"))
  exit(1)
end

logger.info("SNS sender completed")
