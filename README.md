# AWS SNS/SQS Pub-Sub Implementation

A Ruby-based implementation of the publish-subscribe pattern using AWS SNS (Simple Notification Service) and SQS (Simple Queue Service). This project provides a robust messaging system with support for dead letter queues (DLQ) and message retry capabilities.

## Overview

This project implements a complete pub-sub messaging system with three main components:

1. **SNS Publisher** (`sender.rb`): Publishes messages to an SNS topic with support for JSON formatting and custom attributes
2. **SQS Consumer** (`receiver.rb`): Consumes messages from an SQS queue with support for both direct SQS and SNS-forwarded messages
3. **DLQ Processor** (`retry_dlq.rb`): Processes failed messages from a Dead Letter Queue back to their destination queue

## Prerequisites

- Ruby 2.7 or higher
- AWS Account with access to SNS and SQS services
- Configured AWS credentials (either via AWS CLI or environment variables)

## Installation

1. Clone the repository:
   ```bash
   git clone [repository-url]
   cd pub-sub-with-sqs
   ```

2. Install dependencies:
   ```bash
   bundle install
   ```

## Components

### Sender (sender.rb)

The SNS message publisher supports:

- JSON message formatting
- Custom message attributes
- Command-line interface
- File-based message input
- Logging with sender ID tracking

```bash
# Basic usage
./sender.rb --topic-arn "arn:aws:sns:region:account:topic" --message '{"key": "value"}'

# Send message from file
./sender.rb --topic-arn "arn:aws:sns:region:account:topic" --file message.json

# Add custom attributes
./sender.rb --topic-arn "arn:aws:sns:region:account:topic" --file message.json --attribute "priority=high:String"
```

### Receiver (receiver.rb)

The SQS message consumer features:

- Long-polling support
- Message attribute handling
- SNS message format detection
- Automated message deletion after processing
- Configurable batch processing

```bash
# Basic usage
./receiver.rb --queue-url "https://sqs.region.amazonaws.com/account/queue"

# Configure batch processing
./receiver.rb --queue-url "https://sqs.region.amazonaws.com/account/queue" --max-messages 5 --wait-time 10
```

### DLQ Retry Processor (retry_dlq.rb)

The Dead Letter Queue processor supports:

- Message reprocessing from DLQ to destination queue
- Batch processing
- Progress tracking
- Configurable message retention
- Detailed statistics

```bash
# Basic usage
./retry_dlq.rb --dlq-url "https://sqs.region.amazonaws.com/account/dlq" --destination-url "https://sqs.region.amazonaws.com/account/queue"

# Process with custom batch size and delay
./retry_dlq.rb --dlq-url "https://sqs.region.amazonaws.com/account/dlq" --destination-url "https://sqs.region.amazonaws.com/account/queue" --batch-size 5 --delay-seconds 30
```

## Message Format

Messages can be sent in JSON format. Example `message.json`:

```json
{
  "my-message": "hello baby",
  "age": 13
}
```

## Configuration Options

### Sender Options

- `--topic-arn ARN`: SNS Topic ARN (required)
- `--id IDENTIFIER`: Sender identifier
- `--region REGION`: AWS Region (default: us-east-1)
- `--file FILE_PATH`: Load message from JSON file
- `--message JSON_STRING`: Provide message as JSON string
- `--key-value KEY=VALUE`: Add key-value pairs to message
- `--attribute NAME=VALUE:TYPE`: Add message attributes

### Receiver Options

- `--queue-url URL`: SQS Queue URL (required)
- `--id IDENTIFIER`: Receiver identifier
- `--region REGION`: AWS Region (default: us-east-1)
- `--max-messages NUM`: Maximum messages to receive (default: 10)
- `--wait-time SECONDS`: Long polling wait time (default: 20)
- `--visibility-timeout SECONDS`: Message visibility timeout (default: 30)

### DLQ Retry Options

- `--dlq-url URL`: Dead Letter Queue URL (required)
- `--destination-url URL`: Destination Queue URL (required)
- `--batch-size NUM`: Messages per batch (default: 10, max: 10)
- `--delay-seconds NUM`: Visibility delay in destination queue
- `--max-messages NUM`: Maximum messages to process
- `--keep-in-dlq`: Keep messages in DLQ after processing
- `--region REGION`: AWS Region (default: us-east-1)

## Best Practices

1. **Message Format**: Always validate JSON messages before sending
2. **Batch Processing**: Use appropriate batch sizes (5-10 messages) for optimal throughput
3. **Error Handling**: Implement DLQ for handling failed message processing
4. **Monitoring**: Track message processing statistics using the built-in logging

## AWS Setup Requirements

1. Create an SNS topic and SQS queue
2. Subscribe the SQS queue to the SNS topic
3. (Optional) Configure a Dead Letter Queue for failed messages
4. Ensure proper IAM permissions for SNS publishing and SQS operations

## Additional Resources

- [AWS SNS Documentation](https://aws.amazon.com/sns/)
- [AWS SQS Documentation](https://aws.amazon.com/sqs/)
- [Ruby AWS SDK Documentation](https://docs.aws.amazon.com/sdk-for-ruby/v3/api/)

## Contributing

Feel free to submit issues, fork the repository, and create pull requests for any improvements.

## License

[Add your license information here]
