def find_longest_line(log_file):
    max_length = 0
    longest_line = ""

    with open(log_file, 'r') as file:
        for line in file:
            line = line.rstrip('\n')  # Remove newline characters
            line_length = len(line)
            
            if line_length > max_length:
                max_length = line_length
                longest_line = line

    return longest_line, max_length

# Example usage:
log_file_path = 'sitemap_parser.log'  # Specify the path to your log file
longest_line, max_length = find_longest_line(log_file_path)

if longest_line:
    print(f"The longest line is: '{longest_line}'")
    print(f"It has {max_length} characters.")
else:
    print("No lines found in the log file.")