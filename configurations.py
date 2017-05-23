features = [('spinning', ''), ('sleeping', '--features sleeping')]
experiment = [
        ('pingpong blackbox', '--example pingpong', '100 true'),
        ('pingpong', '--example pingpong', '1000000 false'),
        ('bfs', '--example bfs', '10000000 10000000'),
]

configurations = [
        (f, e) for f in features for e in experiment]

for (fn, f), (fe, ex, e) in configurations:
    print fn, ',', fe, ',', ex, '--release', f, ',', '--', e, '-w 4'
