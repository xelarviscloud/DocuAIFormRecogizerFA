const { app } = require("@azure/functions");
var moment = require("moment");
const PDFDocument = require("pdf-lib").PDFDocument;
const { degrees } = require("pdf-lib");
const { BlobServiceClient } = require("@azure/storage-blob");
const {
  AzureKeyCredential,
  DocumentAnalysisClient,
} = require("@azure/ai-form-recognizer");
const { MongoClient } = require("mongodb");
const { ObjectId } = require("mongodb");
const axios = require("axios");

// const mongoClient = new MongoClient(
//   "mongodb+srv://hitesh:hitesh@cluster0.gkpowom.mongodb.net/StaySolveDocuAiDB?retryWrites=true&w=majority&appName=Cluster0"
// );

app.storageQueue("ProcessDocumentToPages-FA", {
  queueName: "%AZURE_DOCUMENT_UPLOAD_QUEUE%",
  connection: "AZURE_DOCUMENTS_STORAGE",
  handler: async (queueItem, context) => {
    try {
      const _blobName = queueItem.metadata.blob;
      const connString = process.env.AZURE_DOCUMENTS_STORAGE;
      const containerName = process.env.AZURE_STORAGE_CONTAINER;
      const frConnString = process.env.FRM_RECOGNIZER_CONNECTION;
      const frAccessKey = process.env.FRM_RECOGNIZER_ACCESSKEY;
      const mongoDBClientConnection = process.env.MONGO_DB_CONNECTION_STRING;
      const mongoDatabaseName = process.env.MONGO_DATABASE_NAME;
      const connAzFnGetTextRotation = process.env.AZ_FN_GET_TEXT_ROTATION;

      context.log(
        "Storage queue function processed work item:",

        mongoDBClientConnection,
        mongoDatabaseName,
        queueItem
      );

      const mongoClient = new MongoClient(mongoDBClientConnection);
      const database = await mongoClient.db(mongoDatabaseName);
      const pageCollection = await database.collection("pages");
      const documentCollection = await database.collection("documents");

      context.log("blobPath", _blobName, connString, containerName);

      await documentCollection.updateOne(
        { _id: new ObjectId(queueItem.documentId) },
        {
          $set: { status: "Processing" },
        }
      );

      const blobServiceClient = await BlobServiceClient.fromConnectionString(
        connString
      );
      // const frConnString =
      //   "https://document-formrecongnizer-fr.cognitiveservices.azure.com/";
      // const frAccessKey = "6959ada697024d8380ae7020fcc83662";

      const client = new DocumentAnalysisClient(
        frConnString,
        new AzureKeyCredential(frAccessKey)
      );

      // Get a reference to a container
      const containerClient = await blobServiceClient.getContainerClient(
        containerName
      );
      // Get a block blob client
      const blockBlobClient = containerClient.getBlockBlobClient(_blobName);
      const data = await blockBlobClient.downloadToBuffer(0);
      const pdfDoc = await PDFDocument.load(data);
      const numberOfPages = pdfDoc.getPages().length;
      context.log("data", numberOfPages);
      const _folder = queueItem.metadata.blob.split("/")[0];
      context.log("_folder", _folder);

      for (let i = 0; i < numberOfPages; i++) {
        let pageIndex = i + 1;
        // Create a new "sub" document
        let subDocument = await PDFDocument.create();
        // copy the page at current index
        let [copiedPage] = await subDocument.copyPages(pdfDoc, [i]);
        let page_Rotation = copiedPage?.getRotation().angle;
        subDocument.addPage(copiedPage);
        let pdfBytes = await subDocument.save();
        // Need to check rotation angle

        let config = {
          method: "post",
          maxBodyLength: Infinity,
          //url: "http://0.0.0.0:7071/api/http_trigger",
          url: connAzFnGetTextRotation,
          // headers: {
          //   ...data.getHeaders(),
          // },
          data: pdfBytes,
        };

        rotation = await axios.request(config);

        console.log("Orientation Response:", rotation?.data);

        if (rotation && rotation?.data && rotation.data > 0) {
          rPage = subDocument.getPage(0);
          rPage.setRotation(degrees(rotation.data));
          subDocument.removePage(0);
          subDocument.addPage(rPage);
          pdfBytes = await subDocument.save();
        }
        let blockBlobClient = containerClient.getBlockBlobClient(
          `${_folder}/Page_${pageIndex}.pdf`
        );

        let uploadBlobResponse = await blockBlobClient.upload(
          pdfBytes,
          pdfBytes.length
        );

        let poller = await client.beginAnalyzeDocument(
          "prebuilt-document",
          pdfBytes
        );

        let result = await poller.pollUntilDone();

        let results = await pageCollection.insertOne({
          documentId: new ObjectId(queueItem.documentId),
          data: result,
          dataContentToSearch: result?.content?.toLocaleLowerCase(),
          pageName: `Page_${pageIndex}`,
          pageBlobPath: `${_folder}/Page_${pageIndex}.pdf`,
          documentName: queueItem.metadata.blob,
          sortId: i,
          locationId: queueItem.locationId,
          organizationId: queueItem.organizationId,
          userId: queueItem.userId,
          userName: queueItem.userName,
          tags: extractTags(result?.keyValuePairs, context),
          createdAt: new Date(),
          pageRotation: page_Rotation,
        });
        await documentCollection.updateOne(
          { _id: new ObjectId(queueItem.documentId) },
          {
            $set: { status: "Completed", pageCount: numberOfPages },
          }
        );
        context.log("Page extraction", `${_folder}/Page_${i}.pdf`);
      }
    } catch (error) {
      context.log("Error:", error);
    }
  },
});

function extractTags(keyvaluePairs, context) {
  const _name = extractName(keyvaluePairs, "name:", context);
  const _phone = extractName(keyvaluePairs, "phone:", context);
  const _confirmationNumber = extractName(
    keyvaluePairs,
    "Confirmation Number:",
    context
  );
  const _arrival = extractName(keyvaluePairs, "Arrival:", context);
  const _departure = extractName(keyvaluePairs, "Departure:", context);

  return {
    name: _name?.toLowerCase(),
    phone: _phone,
    confirmationNumber: _confirmationNumber?.toLowerCase(),
    arrivalDate:
      _arrival?.length > 0
        ? moment(_arrival?.substring(0, 12), "MMM DD, YYYY").format(
            "yyyy-MM-DD"
          )
        : "",
    departureDate:
      _departure?.length > 0
        ? moment(_departure?.substring(0, 12), "MMM DD, YYYY").format(
            "yyyy-MM-DD"
          )
        : "",
    // mdbArrival: new Date(
    //   moment(_arrival?.substring(0, 12), "MMM DD, YYYY").format("yyyy-MM-DD")
    // ),
    // mdbDeparture: new Date(
    //   moment(_departure?.substring(0, 12), "MMM DD, YYYY").format("yyyy-MM-DD")
    // ),
  };
}

function extractName(keyvaluePairs, tagName, context) {
  const _pair = keyvaluePairs.find(
    (key) => key.key.content.toLowerCase() === tagName.toLowerCase()
  );
  if (_pair) {
    return _pair.value?.content;
  }
  return "";
}
