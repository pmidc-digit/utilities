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
    "Financial Year": {
        path: ["credentialSubject", "financialYear"],
        format: (data) => (data)
    },
    "Owner Contact": {
        path: ["credentialSubject", "ownerContact"],
        format: (data) => (data)
    },
    "Owner Name": {
        path: ["credentialSubject", "ownerName"],
        format: (data) => (data)
    },

    "License Number": {
        path: ["credentialSubject", "licenseNumber"],
        format: (data) => (data)
    },
    "Trade Address": {
        path: ["credentialSubject", "tradeAddress", "tradeAddressLocality"],
        format: (data) => (data)
    },
    "Trade Name": {
        path: ["credentialSubject", "tradeName"],
        format: (data) => (data)
    },

    "Trade Type": {
        path: ["credentialSubject", "tradeType"],
        format: (data) => (data)
    }

};
