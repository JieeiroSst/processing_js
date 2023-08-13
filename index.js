const pg = require('pg');

export function createDatabasePool() {
    try {
        const connectionString = `postgres://${USER}:${PASSWORD}@localhost:5432/postgres`;
        const pool = new pg.Pool({ connectionString });
        return pool;
    } catch (error) {
        console.error('Error creating database pool:', error);
        throw error;
    }
}

const queryStream = new QueryStream(
    "SELECT * FROM generate_series(0, $1) num",
    [1000000],
    { batchSize: 1000 }
);

const transformStream = new Transform({
    objectMode: true,
    transform(row, encoding, callback) {
        row.description = `Row ${row.num}`;
        row.date = new Date().toString();
        callback(null, `${row.num}, ${row.description}, ${row.date}` + "\n");
    },
});

const fileWriteStream = fileStream.createWriteStream("output.csv");

const startStream = (transformStream, writeStream) => {
    console.log("STARTED ", new Date());
    pool.connect((err, client, done) => {
        if (err) console.error(err);

        const stream = client.query(queryStream);

        stream
            .pipe(transformStream)
            .pipe(writeStream)
            .on("error", console.error)
            .on("finish", () => {
                console.log("FINISHED: ", new Date());
                done();
            });
    });
};

startStream(transformStream, fileWriteStream);