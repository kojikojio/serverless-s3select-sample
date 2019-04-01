import { S3 } from "aws-sdk";
import { default as s3Stream } from "s3-upload-stream";
import { Handler, Context, Callback } from "aws-lambda";
import { StreamingEventStream } from "aws-sdk/lib/event-stream/event-stream";
import { ProgressEvent } from "aws-sdk/clients/s3";

const PART_SIZE: number = 16 * 1024 * 1024;

interface S3SelectEvent { Records?: S3.RecordsEvent; Stats?: S3.StatsEvent; Progress?: ProgressEvent; Cont?: S3.ContinuationEvent; End?: S3.EndEvent; }

export const handler: Handler = (event: any, context: Context, callback: Callback) => {
    const sql = event.sql
    const bucket = event.bucket;
    const key = event.key;
    const putBucket = event.putBucket;
    const putKey = event.putKey;
    console.log(sql, bucket, key, putBucket, putKey);

    const s3 = new S3();
    const uploadParams = { Bucket: putBucket, Key: putKey };
    const upload = s3Stream(s3).upload(uploadParams);
    upload.maxPartSize(PART_SIZE);

    const params = selectObjectContentParams(bucket, key, sql);
    s3.selectObjectContent(params, (err, data) => {
        if (err) { throw err; }
        const stream = data.Payload! as StreamingEventStream<S3SelectEvent>;
        stream.on("data", (event) => {
            if (event.Records) { upload.write(event.Records.Payload); }
        });
        stream.on("end", () => { upload.end(); });
        stream.on("error", (err) => { throw err; });
    });
    upload.on("uploaded", () => { console.log("end"); callback(null, "End"); });
    upload.on("error", (err) => { console.log(err); callback(err); });
}
function selectObjectContentParams(bucketName: string, objectKey: string, sql: string): S3.SelectObjectContentRequest {
    return {
        Bucket: bucketName,
        Key: objectKey,
        ExpressionType: "SQL",
        Expression: sql,
        InputSerialization: {
            CSV: {
                FileHeaderInfo: "USE",
            },
        },
        OutputSerialization: {
            CSV: {},
        },
    };
}
