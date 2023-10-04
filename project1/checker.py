
def linearizeablilty(file):
    f = open(file)
    lines = f.readlines()

    segments = []
    segment = ""
    for line in lines:
        segment += line
        if line == "\n":
            segments.append(segment)
            segment = ""

    feed = []
    for segment in segments:
        segment_lines = segment.split('\n')
        timestamp = int(segment_lines[-4])
        put = False
        if "Server" in segment_lines[-5]:
            # this is a put request
            value = int(segment_lines[-5].split(' ')[-1])
            put = True
        else:
            value = int(segment_lines[-5])
        feed.append((timestamp, value, put))
        
    feed = sorted(feed, key=lambda x: x[0])
    
    count = 0
    value = 150
    for item in feed:
        if item[2] == True:
            value = item[1]
        else:
            if value != item[1]:
                count += 1
    print("Linearizeability failed for {}%.".format(100.0 * (count) / len(feed)))

def main():
    linearizeablilty("dumped_responses_serial.log")
    linearizeablilty("dumped_responses_parallel.log")



if __name__ == "__main__":
    main()