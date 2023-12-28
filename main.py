
data = {}
with open('./ANA202304070.txt','r') as file:
    contents = file.read()
for line in contents.split('\n'):
    parts = line.split(',')
    
    if parts[0] == 'id':
        data['gameid'] = parts[1]
    elif parts[0] == 'info':
        data[parts[1]] = parts[2]

print(data)