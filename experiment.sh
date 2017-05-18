set -e
set -x

python configurations.py > tmp_configurations

rm output || true

while read -r command
do
  IFS=, read -a fields <<< "$command"
  feat=${fields[0]}
  exper=${fields[1]}
  examp=${fields[2]}
  flags=${fields[3]}
  params=${fields[4]}
  echo $feat $exper $examp $flags $params
  cargo build $flags
  for i in `seq 1 10`; do
    echo $i
    (time -p cargo run $flags $params) 2> tmp_output
    echo $?
    real=`cat tmp_output | grep real | sed 's/real //'`
    user=`cat tmp_output | grep user | sed 's/user //'`
    sys=`cat tmp_output | grep sys | sed 's/sys //'`
    echo $feat, $exper, $examp, 4, $real, $user, $sys >> output
  done
done < tmp_configurations

