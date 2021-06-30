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

export const CertificateDetailsPaths = {

    "Application No": {
        path: ["credentialSubject", "applicationNumber"],
        format: (data) => (data)
    },
    "finyear": {
        path: ["credentialSubject", "financialYear"],
        format: (data) => (data)
    },
    "issuedate": {
        path: ["issuanceDate"],
        format: (data) => (data)
    },
    "ownercontact": {
        path: ["credentialSubject", "ownerContact"],
        format: (data) => (data)
    },
    "ownername": {
        path: ["credentialSubject", "ownerName"],
        format: (data) => (data)
    },

    "tlno": {
        path: ["credentialSubject", "licenseNumber"],
        format: (data) => (data)
    },
    "tradeaddress": {
        path: ["credentialSubject.tradeAddress", "tradeAddressLocality"],
        format: (data) => (data)
    },
    "tradename": {
        path: ["credentialSubject", "tradeName"],
        format: (data) => (data)
    },

    "tradetype": {
        path: ["credentialSubject", "tradeType"],
        format: (data) => (data)
    }

};
