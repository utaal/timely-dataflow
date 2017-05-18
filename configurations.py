features = [('spinning', ''), ('sleeping', '--features sleeping')]
example = [('pingpong', '--example pingpong'), ('bfs', '--example bfs')]
experiment = [('blackbox', '100 true'), ('pingpong', '1000000 false')]

configurations = [
        (f, e, ex) for f in features for e in experiment for ex in example]

for (fn, f), (fe, e), (fex, ex) in configurations:
    print fn, ',', fe, ',', fex, ',', '--release', f, ex, ',', '--', e, '-w 4'
