import sys
output = open("/home/rush/rush-output/output.txt", "w+")
def avg():
    sum,count=0,0
    for line in sys.stdin:
        line = line.split()
        sum+=int(line[0])*int(line[1])
        count+=int(line[1])
    output.write("Average Age: " + str(sum/count))
if __name__ == "__main__":
    avg()