# function to the correct format to visualize the Bytes values
def format_unit_bytes(bytes_value):
    units = ["B", "KB", "MB", "GB", "TB"]
    chosen_unit = 0
    formatted_size = float(bytes_value)

    while formatted_size >= 1024 and chosen_unit < len(units) - 1:
        formatted_size /= 1024.0
        chosen_unit += 1

    return f"{formatted_size:.3f} {units[chosen_unit]}"

# function to read the value from the file. The file has this format: lines of value of Peak Map Physical Memory, empty line, lines of value of Peak Reduce Physical Memory
def read_and_compute_averages(file_path):
    with open(file_path, 'r') as file:
        lines = file.readlines()

    # Split into two blocks (before and after blank line)
    lines = [line.strip() for line in lines]
    if "" not in lines:
        raise ValueError("The file does not contain a blank line to separate the two series.")

    split_index = lines.index("")
    map_values = [int(line) for line in lines[:split_index] if line.strip().isdigit()]
    reduce_values = [int(line) for line in lines[split_index + 1:] if line.strip().isdigit()]

    if not map_values or not reduce_values:
        raise ValueError("One of the data blocks is empty or invalid.")

    avg_map = sum(map_values) / len(map_values)
    avg_reduce = sum(reduce_values) / len(reduce_values)

    print("=== Average Memory Usage ===")
    print(f"Peak Map Physical Memory:    {format_unit_bytes(avg_map)}")
    print(f"Peak Reduce Physical Memory: {format_unit_bytes(avg_reduce)}")

# main function
if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Use: python avg_memory.py <file_input>")
    else:
        read_and_compute_averages(sys.argv[1])
        
# Example usage:
# And then run it like this: python3 avg_memory.py <filepath>