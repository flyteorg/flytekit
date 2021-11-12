set -ex

cat {{ .inputs.f }} >> {{ .outputs.y }}
echo "Hello World {{ .inputs.y }} on  {{ .inputs.j }} - output {{.outputs.x}}"