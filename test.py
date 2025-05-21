import gtfparse

filename = "./samples/gencode_sample.gtf"

with open(filename) as f:
    for i, line in enumerate(f, 1):
        if line.startswith("#"):
            continue
        try:
            # gtfparse expects a file or buffer, but let's try parsing line by line attribute part
            attrs = line.strip().split('\t')[8]
            # Just check if expand_attribute_strings can parse it without error
            from gtfparse.attribute_parsing import expand_attribute_strings
            expand_attribute_strings([attrs])
        except Exception as e:
            print(f"Error parsing line {i}: {line}")
            print(f"Exception: {e}")
            break
