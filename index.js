const path = require('path');
const express = require('express');
const app = express();
const dataproc = require('@google-cloud/dataproc');
const {Storage} = require('@google-cloud/storage');
const sleep = require('sleep');

app.set('view engine', 'pug');
app.set('views', path.join(__dirname, 'views'));

app.use(express.static("static"));

const projectId = 'durable-circle-270223';

// Create storage object
const storage = new Storage({projectId, keyFilename: "./cred.json"});

// Create cluster client
const clusterClient = new dataproc.v1.ClusterControllerClient({
  apiEndpoint: 'us-west1-dataproc.googleapis.com',
  projectId,
  keyFilename: "./cred.json"
});

// Create job client
const jobClient = new dataproc.v1.JobControllerClient({
    apiEndpoint: 'us-west1-dataproc.googleapis.com',
    projectId,
    keyFilename: "./cred.json"
  }); 

// request for cluster
const region = 'us-west1';
const clusterName = 'cluster-6dde';
const request = {
  projectId,
  region,
  clusterName,
};

// Inverted index job
const indexjob = async path => {
    let job = {
        projectId,
        region,
        job: {
            placement: {
                clusterName
            },
            pysparkJob: {
                mainPythonFileUri: 'gs://dataproc-staging-us-west1-22809130792-qxcrlltp/invertedIndex.py',
                args: ['/Data/' + path, 'index']
            },
        },
    };


    // Submit job
    let [jobResp] = await jobClient.submitJob(job);

    // Get job id
    const jobId = jobResp.reference.jobId;

    console.log(`Submitted job "${jobId}"`)

    // Terminal states for a job
    const terminalStates = new Set(['DONE', 'ERROR', 'CANCELLED']);

    // Create a timeout such that the job gets cancelled if not
    // in a termimal state after a fixed period of time.
    const timeout = 600000;
    const start = new Date();

    // Wait for the job to finish.
    const jobReq = {
      projectId: projectId,
      region: region,
      jobId: jobId,
    };

    while (!terminalStates.has(jobResp.status.state)) {
      if (new Date() - timeout > start) {
        await jobClient.cancelJob(jobReq);
        console.log(
          `Job ${jobId} timed out after threshold of ` +
            `${timeout / 60000} minutes.`
        );
        break;
      }
      await sleep.sleep(1);
      [jobResp] = await jobClient.getJob(jobReq);
    }

    // Get cluster
    const [clusterResq] = await clusterClient.getCluster(request);

    // Get output from bucket
    const output = await storage
        .bucket(clusterResq.config.configBucket)
        .file(`outputs/indexOutput/part-00000`)
        .download();

    // Delete output file from bucket
    await storage.bucket(clusterResq.config.configBucket)
        .deleteFiles({ prefix: 'outputs/indexOutput/'}, function(err){})

    // Parse and format output data
    const outputStr = output.toString();
    const outputArr = outputStr.split('\n');

    const items = [];

    outputArr.forEach(el => {
        let elArr = el.split(',');
        items.push(elArr);
    })

   for(let i = 0; i < items.length; i++) {
       for (let j = 0; j < items[i].length; j++) {
           items[i][j] = items[i][j].replace("(", "");
           items[i][j] = items[i][j].replace(")", "");
           items[i][j] = items[i][j].replace(" ", "");
           items[i][j] = items[i][j].replace("u'", "");
           items[i][j] = items[i][j].replace("'", "");
       }
   }

   return items.splice(0, 20);
};

// Term search job
const termjob = async (path, term) => {
    let job = {
        projectId,
        region,
        job: {
            placement: {
                clusterName
            },
            pysparkJob: {
                mainPythonFileUri: 'gs://dataproc-staging-us-west1-22809130792-qxcrlltp/invertedIndex.py',
                args: ['/Data/' + path, 'term', term]
            },
        },
    };

    let [jobResp] = await jobClient.submitJob(job);
    const jobId = jobResp.reference.jobId;

    console.log(`Submitted job "${jobId}"`)

    // Terminal states for a job
    const terminalStates = new Set(['DONE', 'ERROR', 'CANCELLED']);

    // Create a timeout such that the job gets cancelled if not
    // in a termimal state after a fixed period of time.
    const timeout = 600000;
    const start = new Date();

    // Wait for the job to finish.
    const jobReq = {
      projectId: projectId,
      region: region,
      jobId: jobId,
    };

    while (!terminalStates.has(jobResp.status.state)) {
      if (new Date() - timeout > start) {
        await jobClient.cancelJob(jobReq);
        console.log(
          `Job ${jobId} timed out after threshold of ` +
            `${timeout / 60000} minutes.`
        );
        break;
      }
      await sleep.sleep(1);
      [jobResp] = await jobClient.getJob(jobReq);
    }

    const [clusterResq] = await clusterClient.getCluster(request);

    const output = await storage
        .bucket(clusterResq.config.configBucket)
        .file(`outputs/termOutput/part-00000`)
        .download();

    await storage.bucket(clusterResq.config.configBucket)
        .deleteFiles({ prefix: 'outputs/termOutput/'}, function(err){})

    const outputStr = output.toString();
    const outputArr = outputStr.split('\n');

    const items = [];

    outputArr.forEach(el => {
        let elArr = el.split(',');
        items.push(elArr);
    })

   for(let i = 0; i < items.length; i++) {
       for (let j = 0; j < items[i].length; j++) {
           items[i][j] = items[i][j].replace("(", "");
           items[i][j] = items[i][j].replace(")", "");
           items[i][j] = items[i][j].replace(" ", "");
           items[i][j] = items[i][j].replace("u'", "");
           items[i][j] = items[i][j].replace("'", "");
       }
   }

   return items.splice(0, 20);
};

// Top-n job
const topnjob = async (path, n) => {
    let job = {
        projectId,
        region,
        job: {
            placement: {
                clusterName
            },
            pysparkJob: {
                mainPythonFileUri: 'gs://dataproc-staging-us-west1-22809130792-qxcrlltp/invertedIndex.py',
                args: ['/Data/' + path, 'top-n', n]
            },
        },
    };

    let [jobResp] = await jobClient.submitJob(job);
    const jobId = jobResp.reference.jobId;

    console.log(`Submitted job "${jobId}"`)

    // Terminal states for a job
    const terminalStates = new Set(['DONE', 'ERROR', 'CANCELLED']);

    // Create a timeout such that the job gets cancelled if not
    // in a termimal state after a fixed period of time.
    const timeout = 600000;
    const start = new Date();

    // Wait for the job to finish.
    const jobReq = {
      projectId: projectId,
      region: region,
      jobId: jobId,
    };

    while (!terminalStates.has(jobResp.status.state)) {
      if (new Date() - timeout > start) {
        await jobClient.cancelJob(jobReq);
        console.log(
          `Job ${jobId} timed out after threshold of ` +
            `${timeout / 60000} minutes.`
        );
        break;
      }
      await sleep.sleep(1);
      [jobResp] = await jobClient.getJob(jobReq);
    }

    const [clusterResq] = await clusterClient.getCluster(request);

    const output = await storage
        .bucket(clusterResq.config.configBucket)
        .file(`outputs/topOutput/part-00000`)
        .download();

    await storage.bucket(clusterResq.config.configBucket)
        .deleteFiles({ prefix: 'outputs/topOutput/'}, function(err){})

    const outputStr = output.toString();
    const outputArr = outputStr.split('\n');

    const items = [];

    outputArr.forEach(el => {
        let elArr = el.split(',');
        items.push(elArr);
    })

   for(let i = 0; i < items.length; i++) {
       for (let j = 0; j < items[i].length; j++) {
           items[i][j] = items[i][j].replace("(", "");
           items[i][j] = items[i][j].replace(")", "");
           items[i][j] = items[i][j].replace(" ", "");
           items[i][j] = items[i][j].replace("u'", "");
           items[i][j] = items[i][j].replace("'", "");
       }
   }

   return items.splice(0, parseInt(n));
};

app.get('/', (req, res) => {
    res.status(200).render('home', {
        title: 'Home Page'
    })
});

app.get('/service/hugo', async (req, res, next) => {
    try {
        // Submit inverted index job for Hugo files
        let results = await indexjob('Hugo');
      
        res.status(200).render('service', {
            title: 'Services - Hugo',
            results,
            status: 'success',
            name: 'hugo'
        })
    } catch (err) {
        console.log(err)
    }  
});

app.get('/service/shakespeare', async (req, res, next) => {
    try {
        // Submit inverted index job for shakespeare files
        let results = await indexjob('shakespeare');
      
        res.status(200).render('service', {
            title: 'Services - Shakespeare',
            results,
            status: 'success',
            name: 'shakespeare'
        })
    } catch (err) {
        console.log(err)
    }  
});

app.get('/service/tolstoy', async (req, res, next) => {
    try {
        // Submit inverted index job for Tolstoy files
        let results = await indexjob('Tolstoy');
      
        res.status(200).render('service', {
            title: 'Services - Tolstoy',
            results,
            status: 'success',
            name: 'tolstoy'
        })
    } catch (err) {
        console.log(err)
    }  
});

app.get('/term/hugo', async (req, res, next) => {
    // Render input submission page if query length is 0
    if (Object.keys(req.query).length === 0) {
        res.status(200).render('term', {
            title: 'Term Search - Hugo',
            name: 'hugo'
        })
    } else { // Run term search job and render results page if there is a term query
        try {
            // Submit term search job on the value found in query
            let results = await termjob('Hugo', req.query.term);
          
            res.status(200).render('term-res', {
                title: 'Term Search - Hugo',
                term: req.query.term,
                results,
                status: 'success',
                name: 'hugo'
            })
        } catch (err) {
            console.log(err)
        }  
    }

    
});

app.get('/term/shakespeare', async (req, res, next) => {
    
    if (Object.keys(req.query).length === 0) {
        res.status(200).render('term', {
            title: 'Term Search - Shakespeare',
            name: 'shakespeare'
        })
    } else if (Object.keys(req.query).length > 0){
        try {
            let results = await termjob('shakespeare', req.query.term);
          
            res.status(200).render('term-res', {
                title: 'Term Search - Shakespeare',
                results,
                status: 'success',
                term: req.query.term,
                name: 'shakespeare'
            })
        } catch (err) {
            console.log(err)
        }  
    }

    
});

app.get('/term/tolstoy', async (req, res, next) => {

    if (rObject.keys(req.query).length === 0) {
        res.status(200).render('term', {
            title: 'Term Search - Tolstoy',
            name: 'tolstoy'
        })
    } else {
        try {
            let results = await termjob('Tolstoy', req.query.term);
          
            res.status(200).render('term-res', {
                title: 'Term Search - Tolstoy',
                term: req.query.term,
                results,
                status: 'success',
                name: 'tolstoy'
            })
        } catch (err) {
            console.log(err)
        }  
    }
    
});

app.get('/frequency/hugo', async (req, res, next) => {
    // Render input submission page if no query
    if (Object.keys(req.query).length === 0) {
        res.status(200).render('frequency', {
            title: 'Top N - Hugo',
            name: 'hugo'
        })
    } else {
        try { // Submit top-n job with n found in query and render results
            let results = await topnjob('Hugo', req.query.n);
          
            res.status(200).render('freq-res', {
                title: 'Top N - Hugo',
                term: req.query.n,
                results,
                status: 'success',
                name: 'hugo'
            })
        } catch (err) {
            console.log(err)
        }  
    }

});

app.get('/frequency/shakespeare', async (req, res, next) => {

    if (Object.keys(req.query).length === 0) {
        res.status(200).render('frequency', {
            title: 'Top N - Shakespeare',
            name: 'shakespeare'
        })
    } else {
        try {
            let results = await topnjob('shakespeare', req.query.n);
          
            res.status(200).render('freq-res', {
                title: 'Top N - Shakespeare',
                results,
                status: 'success',
                term: req.query.n,
                name: 'shakespeare'
            })
        } catch (err) {
            console.log(err)
        }  
    }    
});

app.get('/frequency/tolstoy', async (req, res, next) => {

    if (Object.keys(req.query).length === 0) {
        res.status(200).render('frequency', {
            title: 'Top N - Tolstoy',
            name: 'tolstoy'
        })
    } else {
        try {
            let results = await topnjob('Tolstoy', req.query.n);
          
            res.status(200).render('freq-res', {
                title: 'Top N - Tolstoy',
                term: req.query.n,
                results,
                status: 'success',
                name: 'tolstoy'
            })
        } catch (err) {
            console.log(err)
        }  
    }
});

app.listen(8081, () => {
    console.log('app is listening...');
})
