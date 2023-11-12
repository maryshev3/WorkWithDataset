file1 = open('dataset.csv', 'r')

lines = file1.readlines()

file1.close

# итерация по строкам
line_i = 0
while line_i < len(lines):
    line_list = list(lines[line_i])

    count_separator = 0
    i = 0
    while i < len(line_list):
        if line_list[i] == ';' and count_separator < 4:
            count_separator += 1
        else:
            if line_list[i] == ';':
                line_list[i] = ','
        i += 1
    lines[line_i] = ''.join(line_list)
    line_i += 1

with open("otus.csv", "w") as file:
    for line in lines:
        file.write(line)