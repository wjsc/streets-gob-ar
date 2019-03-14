const fs = require('fs');
const fetch = require('node-fetch');

const URL = 'https://infra.datos.gob.ar/catalog/modernizacion/dataset/7/distribution/7.6/download/calles.json';
const FILE = './calles.json';
const SEPARATOR = '~';
const ENTER = '\n';

const readFile = file => new Promise((resolve, reject) => {
  fs.readFile( file, 'utf8', (err, data) => {
    if(err) reject(err);
    resolve(data);
  })
})

const getData = async (url, file) => {
  try{
    return JSON.parse(await readFile(file));
  }
  catch(err){
    console.error(err);
    return await fetch(url).then(res => res.ok && res.json());
  }
}

const writeFile = (file, content) => new Promise((resolve, reject) => {
  fs.writeFile(file, content, 'utf8', (err, result) => {
    if(err) reject(err);
    resolve(result)
  })
})

const appendFile = (file, content) => new Promise((resolve, reject) => {
  fs.appendFile(file, content, 'utf8', (err, result) => {
    if(err) reject(err);
    resolve(result)
  })
})


const formatRow = obj => {
  return [ 
    obj.nomenclatura,
    obj.id,
    obj.nombre,
    obj.categoria,
    obj.fuente,
    obj.altura.inicio.derecha,
    obj.altura.inicio.izquierda,
    obj.altura.fin.derecha,
    obj.altura.fin.izquierda,
    obj.departamento.id,
    obj.departamento.nombre,
    obj.provincia.id,
    obj.provincia.nombre,
    obj.geometria.type,
    obj.geometria.coordinates[0][0],
    obj.geometria.coordinates[0][1],
  ].join(SEPARATOR);
}

const getTitles = () => {
  return [
    'nomenclatura',
    'id',
    'nombre',
    'categoria',
    'fuente',
    'altura.inicio.derecha',
    'altura.inicio.izquierda',
    'altura.fin.derecha',
    'altura.fin.izquierda',
    'departamento.id',
    'departamento.nombre',
    'provincia.id',
    'provincia.nombre',
    'geometria.type',
    'geometria.coordinates[0][0]',
    'geometria.coordinates[0][1]',
  ].join(SEPARATOR);
}

const run = async (url, inputFile) => {
  try{
    const { timestamp, version, datos } = await getData(url, inputFile);
    const outputFile = `${timestamp}-${version}.csv`;
    await writeFile(outputFile, getTitles() + ENTER);
    await Promise.all(datos.map( dato => appendFile(outputFile, formatRow(dato) + ENTER)));
    console.log('Process finished');
    process.exit(0);
  }
  catch(err){
    console.log('Process finished with errors');
    console.error(err);
    process.exit(1);
  }
}

run(URL, FILE);