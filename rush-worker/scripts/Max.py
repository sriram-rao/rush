import sys
def max():
    name, max = "", -1
    for line in sys.stdin:
        line = line.strip().rsplit("\t",1)
        if(int(line[1]) > max):
            max = int(line[1])
            name = line[0]

    print(sys.argv[1], name, max)

if __name__ == "__main__":
    max()


