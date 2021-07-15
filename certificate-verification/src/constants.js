const monthNames = [
    "Jan", "Feb", "Mar", "Apr",
    "May", "Jun", "Jul", "Aug",
    "Sep", "Oct", "Nov", "Dec"
];

export function formatDate(givenDate) {
    const dob = new Date(givenDate);
    let day = dob.getDate();
    let monthName = monthNames[dob.getMonth()];
    let year = dob.getFullYear();

    return `${day}-${monthName}-${year}`;
}
/*
function fetchMessageFromLocalization(localizationCode) {
    var myHeaders = new Headers();
    myHeaders.append("Content-Type", "application/json");

    var raw = JSON.stringify({"RequestInfo":{"apiId":"emp","ver":"1.0","ts":"10-03-2017 00:00:00","action":"create","did":"1","key":"abcdkey","msgId":"20170310130900","requesterId":"rajesh","authToken":"{{authToken}}","userInfo":{"id":1}},"tenantId":"pb"});
    localStorage.set("localizationCode", "");
    var requestOptions = {
      method: 'POST',
      headers: myHeaders,
      body: raw,
      redirect: 'follow'
    };
    var finalResponse = "NA";
    var url = "http://localhost:4200/localization/messages/v1/_search?locale=en_IN&tenantId=pb&module=rainmaker-tl";
    //var param = "&codes=" + localizationCode;
    var param = "&codes=" + "TL_GOODS.WHOLESALE.TST-134";
    url = url.concat(param);
    var localizationMessage = "NA";
    finalResponse = fetch(url, requestOptions)
      .then(response => response.text())
     // .then(result => console.log(result))
      .then(result => JSON.parse(result).messages[0].message)
      .catch(error => console.log('error', error));
    return "Beer Bar";
}
*/

export const CertificateDetailsPaths = {

    "License Number": {
        path: ["credentialSubject", "licenseNumber"],
        format: (data) => (data)
    },
    "Application Number": {
        path: ["credentialSubject", "applicationNumber"],
        format: (data) => (data)
    },
    "Financial Year": {
        path: ["credentialSubject", "financialYear"],
        format: (data) => (data)
    },
    "Trade Name": {
        path: ["credentialSubject", "tradeName"],
        format: (data) => (data)
    },
    "Trade Owner Name": {
        path: ["credentialSubject", "ownerName"],
        format: (data) => (data)
    },
    "Trade Address": {
        path: ["credentialSubject", "tradeAddress", "tradeAddressLocality"],
        format: (data) => (data)
    },
    "Trade Type": {
        path: ["credentialSubject", "TradeTypeMessage"],
        format: (data) => (data)
    },
    "License Issue Date":{
        path: ["credentialSubject", "issuedDate"],
        format: (data) => (formatDate(data))
    },
    "License Valid From":{
        path: ["credentialSubject", "validFrom"],
        format: (data) => (formatDate(data))
    },
    "License Valid Till":{
        path: ["credentialSubject", "validTo"],
        format: (data) => (formatDate(data))
    }
};
