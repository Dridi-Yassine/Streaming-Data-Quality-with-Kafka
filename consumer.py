from kafka import KafkaConsumer, KafkaProducer
import json
from typing import Dict, Any, Tuple, Optional

def create_consumer():
    return KafkaConsumer(
        'raw-flights',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='flight-data-quality-group',
        value_deserializer=lambda x: x.decode('utf-8')
    )

def create_producer():
    return KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def validate_record(data: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    """
    Validate a flight record for schema compliance and data quality.

    Returns:
        Tuple[bool, Optional[str]]: (is_valid, error_reason)
    """
    # Check required fields exist
    required_fields = ['ORIGIN_COUNTRY_NAME', 'DEST_COUNTRY_NAME', 'count']

    for field in required_fields:
        if field not in data:
            return False, f"Missing required field: {field}"

    # Validate ORIGIN_COUNTRY_NAME
    origin = data.get('ORIGIN_COUNTRY_NAME')
    if origin is None:
        return False, "ORIGIN_COUNTRY_NAME is null"
    if not isinstance(origin, str):
        return False, f"ORIGIN_COUNTRY_NAME must be string, got {type(origin).__name__}"
    if origin.strip() == "":
        return False, "ORIGIN_COUNTRY_NAME is empty"

    # Validate DEST_COUNTRY_NAME
    dest = data.get('DEST_COUNTRY_NAME')
    if dest is None:
        return False, "DEST_COUNTRY_NAME is null"
    if not isinstance(dest, str):
        return False, f"DEST_COUNTRY_NAME must be string, got {type(dest).__name__}"
    if dest.strip() == "":
        return False, "DEST_COUNTRY_NAME is empty"

    # Validate count field
    count = data.get('count')
    if count is None:
        return False, "count is null"

    # Check if count is an integer or can be converted to one
    if isinstance(count, str):
        # Try to parse string to int
        try:
            count = int(count.strip())
        except ValueError:
            return False, f"count field is not a valid integer: '{data.get('count')}'"
    elif isinstance(count, float):
        # Convert float to int if it's a whole number
        if count.is_integer():
            count = int(count)
        else:
            return False, f"count field must be an integer, got float: {count}"
    elif not isinstance(count, int):
        return False, f"count field must be an integer, got {type(count).__name__}"

    # Check for negative count
    if count < 0:
        return False, f"count cannot be negative: {count}"

    # All validations passed
    return True, None

def parse_json_safely(raw_message: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """
    Safely parse JSON string.

    Returns:
        Tuple[Optional[Dict], Optional[str]]: (parsed_data, error_reason)
    """
    try:
        data = json.loads(raw_message)
        if not isinstance(data, dict):
            return None, f"Expected JSON object, got {type(data).__name__}"
        return data, None
    except json.JSONDecodeError as e:
        return None, f"Invalid JSON: {str(e)}"
    except Exception as e:
        return None, f"Unexpected error parsing JSON: {str(e)}"

def process_message(raw_message: str, producer: KafkaProducer, stats: Dict[str, int]):
    """
    Process a single message from Kafka, validate it, and route to appropriate topic.

    The consumer must not crash on bad data (constraint).
    """
    try:
        # Step 1: Parse JSON safely
        parsed_data, parse_error = parse_json_safely(raw_message)

        if parsed_data is None:
            # Invalid JSON - route to invalid topic
            invalid_record = {
                'raw_message': raw_message[:200],  # Limit size
                'error_type': 'PARSE_ERROR',
                'error_reason': parse_error
            }
            producer.send('invalid-flights', value=invalid_record)
            stats['invalid'] += 1
            stats['parse_errors'] += 1
            print(f"❌ INVALID (Parse Error): {parse_error}")
            return

        # Step 2: Validate schema and data types
        is_valid, validation_error = validate_record(parsed_data)

        if not is_valid:
            # Schema/validation error - route to invalid topic
            invalid_record = {
                'raw_message': raw_message[:200],
                'parsed_data': parsed_data,
                'error_type': 'VALIDATION_ERROR',
                'error_reason': validation_error
            }
            producer.send('invalid-flights', value=invalid_record)
            stats['invalid'] += 1
            stats['validation_errors'] += 1
            print(f"❌ INVALID (Validation Error): {validation_error}")
            return

        # Step 3: Clean and normalize the valid record
        # Only keep the required fields and normalize count to integer
        count = parsed_data['count']
        if isinstance(count, str):
            count = int(count.strip())

        clean_record = {
            'ORIGIN_COUNTRY_NAME': parsed_data['ORIGIN_COUNTRY_NAME'].strip(),
            'DEST_COUNTRY_NAME': parsed_data['DEST_COUNTRY_NAME'].strip(),
            'count': count
        }

        # Route to valid topic
        producer.send('valid-flights', value=clean_record)
        stats['valid'] += 1
        print(f"✅ VALID: {clean_record['ORIGIN_COUNTRY_NAME']} → {clean_record['DEST_COUNTRY_NAME']} ({clean_record['count']} flights)")

    except Exception as e:
        # Catch-all to ensure consumer doesn't crash (constraint)
        stats['unexpected_errors'] += 1
        print(f"❌ UNEXPECTED ERROR: {str(e)} | Raw message: {raw_message[:100]}")
        try:
            error_record = {
                'raw_message': raw_message[:200],
                'error_type': 'UNEXPECTED_ERROR',
                'error_reason': str(e)
            }
            producer.send('invalid-flights', value=error_record)
        except:
            pass  # Even if we can't send to Kafka, don't crash

def main():
    print("=" * 80)
    print("Kafka Flight Data Quality Consumer")
    print("=" * 80)
    print("\nTopics:")
    print("  - Input: raw-flights")
    print("  - Output (valid): valid-flights")
    print("  - Output (invalid): invalid-flights")
    print("=" * 80)

    consumer = create_consumer()
    producer = create_producer()

    # Statistics
    stats = {
        'valid': 0,
        'invalid': 0,
        'parse_errors': 0,
        'validation_errors': 0,
        'unexpected_errors': 0,
        'total': 0
    }

    print("\nStarting to consume messages...\n")

    try:
        for message in consumer:
            stats['total'] += 1
            raw_message = message.value

            print(f"\n[Message #{stats['total']}] ", end="")
            process_message(raw_message, producer, stats)

            # Flush producer periodically
            if stats['total'] % 10 == 0:
                producer.flush()
                print(f"\n📊 Progress: {stats['valid']} valid, {stats['invalid']} invalid")

    except KeyboardInterrupt:
        print("\n\n⏹️  Consumer stopped by user")

    finally:
        # Final statistics
        producer.flush()
        consumer.close()
        producer.close()

        print("\n" + "=" * 80)
        print("FINAL STATISTICS")
        print("=" * 80)
        print(f"Total messages processed: {stats['total']}")
        print(f"Valid records: {stats['valid']} ({stats['valid']/stats['total']*100:.1f}%)" if stats['total'] > 0 else "Valid records: 0")
        print(f"Invalid records: {stats['invalid']} ({stats['invalid']/stats['total']*100:.1f}%)" if stats['total'] > 0 else "Invalid records: 0")
        print(f"  - Parse errors: {stats['parse_errors']}")
        print(f"  - Validation errors: {stats['validation_errors']}")
        print(f"  - Unexpected errors: {stats['unexpected_errors']}")
        print("=" * 80)

if __name__ == "__main__":
    main()
