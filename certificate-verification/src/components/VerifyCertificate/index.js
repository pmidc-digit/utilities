import React, {useState} from "react";
import "./index.css";
import VerifyCertificateImg from "../../assets/img/verify-certificate.png"
import SampleCertificateImg from "../../assets/img/sample_ceritificate.png"
import QRCodeImg from "../../assets/img/qr-code.svg"
import {CertificateStatus} from "../CertificateStatus";
import {CustomButton} from "../CustomButton";
import QRScanner from "../QRScanner";
import JSZip from "jszip";
import Container from "react-bootstrap/Container";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
export const CERTIFICATE_FILE = "certificate.json";

export const VerifyCertificate = () => {
    const [result, setResult] = useState("");
    const [showScanner, setShowScanner] = useState(false);
    const handleScan = data => {
        if (data) {
            const zip = new JSZip();
            zip.loadAsync(data).then((contents) => {
                return contents.files[CERTIFICATE_FILE].async('text')
            }).then(function (contents) {
                setResult(contents)
            }).catch(err => {
                    setResult(data)
                }
            );

        }
    };
    const handleError = err => {
        console.error(err)
    };
    return (
        <div className="container-fluid verify-certificate-wrapper">
            {
                !result &&
                <>
                    {!showScanner &&
                    <>
                        <img src={VerifyCertificateImg} className="banner-img" alt="banner-img"/>
                        <h3 className="text-center">Verify a trade license certificate</h3>
                        <CustomButton className="green-btn" onClick={() => setShowScanner(true)}>
                            <span>Scan QR code</span>
                            <img className="ml-3" src={QRCodeImg} alt={""}/>
                        </CustomButton>
                         <Container className="mt-2 p-4">
                            <p>
                                Once the Trade License flow is completed and the license is approved, a Trade License certificate is issued to the respective trade owners.
                                The Trade License certificate has a secure QR code to protect it against falsification.
                                The genuineness of the certificate can be authenticated from this portal.
                            </p>
                            <p style={{color:"#646D82"}}>Steps for verification</p>
                            <ol className="verify-steps">
                                <li>Click on “Scan QR code” above</li>
                                <li>A notification will prompt to activate your device’s camera</li>
                                <li>Point the camera to the QR code on the certificate issued and scan</li>
                                <li>On successful verification, the following will be displayed
                                <Row>
                                    <Col>
                                        <ul className="success-verify">
                                            <li>Message “Trade License Certificate Successfully Verified”</li>
                                            <li>License Number</li>
                                            <li>Application Number</li>
                                            <li>Financial Year</li>
                                            <li>Trade Name</li>
                                            <li>Trade Owner Name</li>
                                            <li>Trade Owner Address</li>
                                            <li>Trade Type</li>
                                            <li>License Issue Date</li>
                                            <li>License Valid From</li>
                                            <li>License Valid To</li>
                                        </ul>
                                    </Col>
                                </Row>
                                </li>
                                <li>If the certificate is not genuine, “Trade License Certificate Invalid” will be displayed</li>
                                </ol>
                         </Container>
                    </>}
                    {showScanner &&
                    <>
                        <QRScanner onError={handleError}
                                   onScan={handleScan}/>
                        <CustomButton className="green-btn" onClick={() => setShowScanner(false)}>BACK</CustomButton>
                    </>
                    }
                </>
            }
            {
                result && <CertificateStatus certificateData={result} goBack={() => {
                    setShowScanner(false);
                    setResult("");
                }
                }/>
            }


        </div>
    )
};
