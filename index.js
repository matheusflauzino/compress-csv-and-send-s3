import fs from 'node:fs';
import zlib from 'node:zlib';

import AWS from 'aws-sdk';

// Configuração do cliente AWS S3
AWS.config.update({ region: 'sua-regiao-aqui' }); // Substitua com a região desejada
const s3 = new AWS.S3({
  accessKeyId: '',
  secretAccessKey: '',
  region: 'us-east-1',
  correctClockSkew: true
});

function createTempCSVFromArray(data, filename) {
  console.log('create')
    const csvData = data.map((row) => `${row.coluna1};${row.coluna2}`).join('\n');
    const exist = fs.existsSync('./tmp');
    if(!exist) {
      fs.mkdir('./tmp')
    }
    const tempFilePath = `./tmp/${filename}.csv`;
    fs.writeFileSync(tempFilePath, csvData);
    return tempFilePath;
}

function compressFile(inputFile) {
  console.log('compress')
    const outputFile = `${inputFile}.gz`;
    const input = fs.createReadStream(inputFile);
    const output = fs.createWriteStream(outputFile);
    const gzip = zlib.createGzip();
  
    input.pipe(gzip).pipe(output);
  
    // Remover o arquivo original após a compressão
    input.on('end', () => {
      //fs.unlinkSync(inputFile);
    });
  
    return outputFile;
}

async function uploadToS3(bucketName, key, file) {
  console.log('upload')
  const params = {
    Bucket: bucketName,
    Key: key,
    Body: fs.createReadStream(file)
  }

  
  s3.upload(params, (err, data) => {
    if (err) {
      console.error('Erro ao enviar arquivo para o S3:', err);
    } else {
      console.log('Arquivo enviado com sucesso para o S3:', data.Location);
      // Remover o arquivo temporário após o envio
    }

    fs.unlinkSync(file);

  });
  

  console.log(JSON.stringify(params));

  async function delay(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

//  await delay(3000);
  console.log('fim')

  //fs.unlinkSync(file);
}


// Exemplo de uso
const dataArray = [
    { coluna1: 'valor1', coluna2: 'valor2' },
    { coluna1: 'valor3', coluna2: 'valor4' },
    // Adicione mais dados conforme necessário
  ];
const fileName = `SLC1001_${Date.now()}`
const csv = createTempCSVFromArray(dataArray,fileName);
const gzipfile = compressFile(csv);

await uploadToS3('maf-bucket-teste',`pasta/${fileName}.csv.gz`, gzipfile);