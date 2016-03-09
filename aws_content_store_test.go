package main

import (
	"bytes"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"io/ioutil"
	"os"
	"testing"
)

var awsContentStore *AwsContentStore
var oid = "6ae8a75555209fd6c44157c0aed8016e763ff435a19cf186f76863140143ff72"

func TestAwsContentStorePut(t *testing.T) {
	setupAwsTest()
	defer teardownAwsTest()
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	m := &MetaObject{
		Oid:  "6ae8a75555209fd6c44157c0aed8016e763ff435a19cf186f76863140143ff72",
		Size: 12,
	}

	b := bytes.NewBuffer([]byte("test content"))

	if err := awsContentStore.Put(m, b); err != nil {
		t.Fatalf("expected put to succeed, got: %s", err)
	}

	if err := awsContentStore.Exists(m); !err {
		t.Fatalf("expected content to exist after putting")
	}
}

func TestAwsContentStorePutHashMismatch(t *testing.T) {
	setupAwsTest()
	defer teardownAwsTest()
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	m := &MetaObject{
		Oid:  "6ae8a75555209fd6c44157c0aed8016e763ff435a19cf186f76863140143ff72",
		Size: 12,
	}

	b := bytes.NewBuffer([]byte("bogus content"))

	if err := awsContentStore.Put(m, b); err == nil {
		t.Fatal("expected put with bogus content to fail")
	}
}

func TestAwsContentStorePutSizeMismatch(t *testing.T) {
	setupAwsTest()
	defer teardownAwsTest()
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	m := &MetaObject{
		Oid:  "6ae8a75555209fd6c44157c0aed8016e763ff435a19cf186f76863140143ff72",
		Size: 14,
	}

	b := bytes.NewBuffer([]byte("test content"))

	if err := awsContentStore.Put(m, b); err == nil {
		t.Fatal("expected put with bogus size to fail")
	}

}

func TestAwsContentStoreGet(t *testing.T) {
	setupAwsTest()
	defer teardownAwsTest()
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	content := []byte("test content")
	m := &MetaObject{
		Oid:  "6ae8a75555209fd6c44157c0aed8016e763ff435a19cf186f76863140143ff72",
		Size: 12,
	}

	b := bytes.NewBuffer(content)

	if err := awsContentStore.Put(m, b); err != nil {
		t.Fatalf("expected put to succeed, got: %s", err)
	}

	r, err := awsContentStore.Get(m)
	if err != nil {
		t.Fatalf("expected get to succeed, got: '%s'", err)
	}

	by, err := ioutil.ReadAll(r)
	if err != nil {
		t.Fatalf("Err: '%v'", err.Error())
	}

	if string(by) != "test content" {
		t.Fatalf("expected to read content, got: '%s'", string(by))
	}

	// Failures
	if _, ferr := awsContentStore.Get(&MetaObject{Oid: "Nothing really here", Size: 1}); ferr == nil {
		t.Fatalf("Expected an error but got nothing")
	}

}

func TestAwsContentStoreGetWithPublicAcl(t *testing.T) {
	setupAwsTest()
	defer teardownAwsTest()
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	Config.Aws.BucketAcl = "public-read"
	content := []byte("test content")
	m := &MetaObject{
		Oid:  "6ae8a75555209fd6c44157c0aed8016e763ff435a19cf186f76863140143ff72",
		Size: 12,
	}

	b := bytes.NewBuffer(content)

	if err := awsContentStore.Put(m, b); err != nil {
		t.Fatalf("expected put to succeed, got: %s", err)
	}

	r, err := awsContentStore.Get(m)
	if err != nil {
		t.Fatalf("expected get to succeed, got: '%s'", err)
	}

	by, err := ioutil.ReadAll(r)
	if err != nil {
		t.Fatalf("Err: '%v'", err.Error())
	}

	if string(by) != "test content" {
		t.Fatalf("expected to read content, got: '%s'", string(by))
	}

	// Failures
	if _, ferr := awsContentStore.Get(&MetaObject{Oid: "Nothing really here", Size: 1}); ferr == nil {
		t.Fatalf("Expected an error but got nothing")
	}

}

func TestAwsContentStoreGetNonExisting(t *testing.T) {
	setupAwsTest()
	defer teardownAwsTest()
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	_, err := awsContentStore.Get(&MetaObject{Oid: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"})
	if err == nil {
		t.Fatalf("expected to get an error, but content existed")
	}
}

func TestAwsContentStoreExists(t *testing.T) {
	setupAwsTest()
	defer teardownAwsTest()
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	m := &MetaObject{
		Oid:  "6ae8a75555209fd6c44157c0aed8016e763ff435a19cf186f76863140143ff72",
		Size: 12,
	}

	b := bytes.NewBuffer([]byte("test content"))

	if awsContentStore.Exists(m) {
		t.Fatalf("expected content to not exist yet")
	}

	if err := awsContentStore.Put(m, b); err != nil {
		t.Fatalf("expected put to succeed, got: %s", err)
	}

	if !awsContentStore.Exists(m) {
		t.Fatalf("expected content to exist")
	}
}

func TestAwsAcls(t *testing.T) {
	setupAwsTest()
	defer teardownAwsTest()
	Config.Aws.BucketAcl = "private"
	awsContentStore.setAcl()
	if awsContentStore.acl != s3.BucketCannedACLPrivate {
		t.Fatalf("Should have been set to private, but got %s", awsContentStore.acl)
	}
	Config.Aws.BucketAcl = "public-read"
	awsContentStore.setAcl()
	if awsContentStore.acl != s3.BucketCannedACLPublicRead {
		t.Fatalf("Should have been set to public-read, but got %s", awsContentStore.acl)
	}
	Config.Aws.BucketAcl = "public-read-write"
	awsContentStore.setAcl()
	if awsContentStore.acl != s3.BucketCannedACLPublicReadWrite {
		t.Fatalf("Should have been set to public-read-write, but got %s", awsContentStore.acl)
	}
	Config.Aws.BucketAcl = "authenticated-read"
	awsContentStore.setAcl()
	if awsContentStore.acl != s3.BucketCannedACLAuthenticatedRead {
		t.Fatalf("Should have been set to authenticated-read, but got %s", awsContentStore.acl)
	}
	Config.Aws.BucketAcl = "bucket-owner-full-control"
	awsContentStore.setAcl()
	if awsContentStore.acl != s3.ObjectCannedACLBucketOwnerFullControl {
		t.Fatalf("Should have been set to bucket-owner-full-control, but got %s", awsContentStore.acl)
	}
}

func clientSession() *s3.S3 {
	os.Setenv("AWS_ACCESS_KEY_ID", Config.Aws.AccessKeyId)
	os.Setenv("AWS_SECRET_ACCESS_KEY", Config.Aws.SecretAccessKey)
	session := session.New(&aws.Config{Region: &Config.Aws.Region})
	client := s3.New(session)
	return client
}

func setupAwsTest() {
	store, err := NewAwsContentStore()
	if err != nil {
		fmt.Printf("error initializing content store: %s\n", err)
		os.Exit(1)
	}
	awsContentStore = store
}

func teardownAwsTest() {
	client := clientSession()
	client.DeleteObject(&s3.DeleteObjectInput{Bucket: &Config.Aws.BucketName, Key: aws.String(transformKey(oid))})
	client.WaitUntilObjectNotExists(&s3.HeadObjectInput{Bucket: &Config.Aws.BucketName, Key: aws.String(transformKey(oid))})
}
