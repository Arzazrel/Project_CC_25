import sys
# program that given the output file from the word count job done in map reduce on the project input extracts the 10 most and least frequent words.

# main function
def main():
    
    if len(sys.argv) < 2:       # check the number of argument
        print("Usage: python wordcount_stats.py <output_file_path>")
        sys.exit(1)

    file_path = sys.argv[1]     # take the file 
    word_counts = {}            # dictionary for pair (word, occurrence)
    tot_word_count = 0          # the total number of word
    
    with open(file_path, 'r') as file:          # read the file
        for line in file:                       # scroll the lines
            parts = line.strip().split("\t")    # split the line
            if len(parts) == 2:                 # check the numberof the split
                word = parts[0]                 # take word
                tot_word_count += 1             # update the counter
                try:
                    count = int(parts[1])       # take occurrence
                    word_counts[word] = count   # put in the dictionay
                except ValueError:
                    pass                        # ignore malformed lines

    sorted_items = sorted(word_counts.items(), key=lambda x: x[1])  # sort by frequency

    print("total number of word: ",tot_word_count)

    print("=== 10 least frequent words ===")
    for word, count in sorted_items[:10]:           # print the 10 least frequent words
        print(f"{word} : {count}")

    print("\n=== 10 most frequent words ===")
    for word, count in sorted_items[-10:][::-1]:    # print the 10 most frequent words
        print(f"{word} : {count}")

if __name__ == "__main__":
    main()