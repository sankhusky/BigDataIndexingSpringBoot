package com.sanket.demo7255.security;

import com.nimbusds.jose.*;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.crypto.RSASSAVerifier;
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;

import java.text.ParseException;
import java.util.Date;

public class JWTToken {

    private RSAKey rsaPublicKey;

    public String generateToken() throws JOSEException {
        RSAKey rsaKey = new RSAKeyGenerator(2048)
                .keyID("123")
                .generate();
        this.rsaPublicKey = rsaKey.toPublicJWK();

        // Create RSA-signer with the private key
        JWSSigner signer = new RSASSASigner(rsaKey);

        // Prepare JWT with claims set with 10 mins ttl
        JWTClaimsSet claimsSet = new JWTClaimsSet.Builder()
                .expirationTime(new Date(new Date().getTime() + 600 * 1000))
                .build();

        SignedJWT signedJWT = new SignedJWT(
                new JWSHeader.Builder(JWSAlgorithm.RS256).keyID(rsaKey.getKeyID()).build(),
                claimsSet);

        // Compute the RSA signature
        signedJWT.sign(signer);

        return signedJWT.serialize();
    }

    public boolean isTokenValid(String jwtToken){

        if (this.rsaPublicKey == null) {
            return false;
        }

        try {
            SignedJWT signedJWT = SignedJWT.parse(jwtToken);

            JWSVerifier verifier = new RSASSAVerifier(this.rsaPublicKey);

            // token is not valid
            if(!signedJWT.verify(verifier)){
                return false;
            }

            Date expirationTime = signedJWT.getJWTClaimsSet().getExpirationTime();

            // token ttl has expired
            if(new Date().after(expirationTime)) {
                return false;
            }
        } catch (Exception e) {
            return false;
        }

        return true;
    }
}
