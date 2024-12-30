from struct import pack
import sys

data = [
    [
        b'hello world',
        b'this is a test'
    ],
    [
        b'testing',
        b'1 2 3'
    ]
]

for i, d in enumerate(data):
    with open(f'{sys.argv[1]}/data{i}','wb') as f:
        f.write(b'DD') # Magic bytes
        f.write(pack('<I', len(d)))
        for dd in d:
            f.write(pack('<I', len(dd)))
            f.write(dd)
