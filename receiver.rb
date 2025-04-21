#!/usr/bin/env ruby

require 'aws-sdk-sqs'
require 'logger'
require 'optparse'
require 'json'

# Parse command line arguments
options = {
  queue_url: nil,
  receiver_id: nil,
  region: 'us-east-1',
  max_messages: 10,
  wait_time: 20,
  visibility_timeout: 30
}

# Parse command line options
OptionParser.new do |opts|
  opts.banner = "Usage: #{$PROGRAM_NAME} [options]"
  
  opts.on('-q', '--queue-url URL', 'SQS Queue URL') do |url|
    options[:queue_url] = url
  end
  
  opts.on('-i', '--id IDENTIFIER', 'Receiver identifier (defaults to queue name)') do |id|
    options[:receiver_id] = id
  end
  
  opts.on('-r', '--region REGION', 'AWS Region (defaults to us-east-1)') do |region|
    options[:region] = region
  end
  
  opts.on('-m', '--max-messages NUM', Integer, 'Maximum number of messages to receive at once (defaults to 10)') do |num|
    options[:max_messages] = num
  end
  
  opts.on('-w', '--wait-time SECONDS', Integer, 'Long polling wait time in seconds (defaults to 20)') do |sec|
    options[:wait_time] = sec
  end
  
  opts.on('-t', '--visibility-timeout SECONDS', Integer, 'Message visibility timeout in seconds (defaults to 30)') do |sec|
    options[:visibility_timeout] = sec
  end
  
  opts.on('-h', '--help', 'Display this help message') do
    puts opts
    exit
  end
end.parse!

# Validate required parameters
if options[:queue_url].nil?
  puts "Error: Queue URL is required. Use --queue-url option."
  exit(1)
end

# Extract receiver ID from queue name if not provided
if options[:receiver_id].nil?
  # Extract queue name from URL (last part after the last slash)
  options[:receiver_id] = options[:queue_url].split('/').last
end

# Custom log formatter that includes receiver ID
class ReceiverFormatter < Logger::Formatter
  def initialize(receiver_id)
    @receiver_id = receiver_id
  end
  
  def call(severity, time, progname, msg)
    formatted_time = time.strftime("%Y-%m-%d %H:%M:%S.%L")
    "[#{formatted_time}] [#{severity}] [Receiver: #{@receiver_id}] #{msg}\n"
  end
end

# Initialize logger with custom formatter
logger = Logger.new(STDOUT)
logger.level = Logger::INFO
logger.formatter = ReceiverFormatter.new(options[:receiver_id])

# Log configuration details
logger.info("Starting SQS message receiver")
logger.info("Queue URL: #{options[:queue_url]}")
logger.info("Region: #{options[:region]}")
logger.info("Receiver ID: #{options[:receiver_id]}")
logger.info("Max messages per request: #{options[:max_messages]}")
logger.info("Wait time: #{options[:wait_time]} seconds")
logger.info("Visibility timeout: #{options[:visibility_timeout]} seconds")

begin
  # Initialize AWS SQS client
  sqs = Aws::SQS::Client.new(region: options[:region])
  logger.info("Connected to SQS in region #{options[:region]}")
  
  # Setup signal handler for graceful shutdown
  running = true
  Signal.trap('INT') do
    logger.info('Received interrupt signal. Shutting down gracefully...')
    running = false
  end
  
  # Main message processing loop
  logger.info("Starting message polling loop")
  while running
    begin
      # Request messages from SQS queue
      response = sqs.receive_message(
        queue_url: options[:queue_url],
        max_number_of_messages: options[:max_messages],
        visibility_timeout: options[:visibility_timeout],
        wait_time_seconds: options[:wait_time],
        attribute_names: ['All'],
        message_attribute_names: ['All']
      )
      
      if response.messages.empty?
        logger.info('No messages received')
      else
        logger.info("Received #{response.messages.size} message(s)")
        
        # Process each message
        response.messages.each do |message|
          logger.info("Processing message ID: #{message.message_id}")
          
          # Parse and extract the actual message
          message_body = message.body
          actual_body = message_body
          message_source = "Direct SQS"
          
          # Try to parse as JSON (SNS messages come as JSON)
          begin
            parsed_body = JSON.parse(message_body)
            
            # Check if this is an SNS message (has 'Type', 'Message', etc. fields)
            if parsed_body.is_a?(Hash) && parsed_body['Type'] == 'Notification'
              message_source = "SNS Notification"
              logger.info("Detected SNS message format")
              
              # Extract the actual message content from the SNS wrapper
              actual_body = parsed_body['Message']
              
              # Try to parse the inner message if it's also JSON
              begin
                inner_parsed = JSON.parse(actual_body)
                actual_body = inner_parsed
                message_source = "SNS+JSON"
              rescue JSON::ParserError
                # The inner message is not JSON, keep as string
              end
            end
          rescue JSON::ParserError
            # Not a JSON message, keep the original message body
            logger.info("Message is not in JSON format")
          end
          
          # Print message details
          puts "\n==== Message Details ====="
          puts "Receiver: #{options[:receiver_id]}"
          puts "Message ID: #{message.message_id}"
          puts "Message Source: #{message_source}"

          
          if message_body.length > 200
            puts "Raw Message Body (truncated): #{message_body[0..200]}..."
          else
            puts "Raw Message Body: #{message_body}"
          end
          
          puts "\nProcessed Message Content:"
          puts actual_body.inspect
          
          # Print message attributes if any
          unless message.attributes.empty?
            puts "\nMessage Attributes:"
            message.attributes.each do |name, value|
              puts "  #{name}: #{value}"
            end
          end
          
          # Print message custom attributes if any
          unless message.message_attributes.empty?
            puts "\nCustom Message Attributes:"
            message.message_attributes.each do |name, attr|
              puts "  #{name}: #{attr.string_value || attr.binary_value || attr.data_type}"
            end
          end
          
          puts "========================\n"
          
          # Delete the message from the queue after processing
          sqs.delete_message(
            queue_url: options[:queue_url],
            receipt_handle: message.receipt_handle
          )
          logger.info("Deleted message ID: #{message.message_id}")
        end
      end
      
    rescue Aws::SQS::Errors::ServiceError => e
      logger.error("SQS service error: #{e.message}")
      sleep 5  # Add delay before retrying
    rescue StandardError => e
      logger.error("Unexpected error: #{e.message}")
      logger.error(e.backtrace.join("\n"))
      sleep 5  # Add delay before retrying
    end
  end
  
rescue StandardError => e
  logger.error("Initialization error: #{e.message}")
  logger.error(e.backtrace.join("\n"))
  exit(1)
end

logger.info("SQS receiver shutdown complete")
