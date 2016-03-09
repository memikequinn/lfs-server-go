package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"io"
	"io/ioutil"
	"os"
	"strings"
)

const (
	ContentType = "binary/octet-stream"
)

// ContentStore provides a simple file system based storage.
type AwsContentStore struct {
	client  *s3.S3
	bucket  *s3.Bucket
	session *session.Session
	authId  string
	authKey string
	acl     string
}

// NewContentStore creates a ContentStore at the base directory.
func NewAwsContentStore() (*AwsContentStore, error) {
	os.Setenv("AWS_ACCESS_KEY_ID", Config.Aws.AccessKeyId)
	os.Setenv("AWS_SECRET_ACCESS_KEY", Config.Aws.SecretAccessKey)
	awsConfig := &aws.Config{Region: &Config.Aws.Region, MaxRetries: aws.Int(5)}
	// set an endpoint, if we have one
	// Allows for S3 compatible interfaces
	if Config.Aws.EndpointUrl != "" {
		awsConfig.Endpoint = &Config.Aws.EndpointUrl
	}
	session := session.New(awsConfig)
	client := s3.New(session)
	self := &AwsContentStore{bucket: &s3.Bucket{Name: &Config.Aws.BucketName}, client: client, session: session}
	self.setAcl()
	return self, nil
}

func (s *AwsContentStore) Get(meta *MetaObject) (io.Reader, error) {
	path := transformKey(meta.Oid)
	objReq := &s3.GetObjectInput{
		Bucket: s.bucket.Name,
		Key:    aws.String(path),
	}
	resp, err := s.client.GetObject(objReq)
	if err != nil {
		// handle error
		logger.Log(kv{"fn": "AwsContentStore.Get", "error": ": " + err.Error()})
		return nil, err
	}
	defer resp.Body.Close()
	// We have to buffer here or the reader will close before we can consume it.
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		logger.Log(kv{"fn": "AwsContentStore.Get", "error": ": " + err.Error()})
	}
	return bytes.NewReader(data), nil
}

// TODO: maybe take write errors into account and buffer/resend to amazon?
func (s *AwsContentStore) Put(meta *MetaObject, r io.Reader) error {
	path := transformKey(meta.Oid)
	/*
		There is probably a better way to compute this but we need to write the file to memory to
		 compute the sha256 value and make sure what we're writing is correct.
		 If not, git wont be able to find it later
	*/
	hash := sha256.New()
	buf, _ := ioutil.ReadAll(r)
	hw := io.MultiWriter(hash)
	written, err := io.Copy(hw, bytes.NewReader(buf))
	if err != nil {
		logger.Log(kv{"fn": "AwsContentStore.Put", "error": ": " + err.Error()})
		return err
	}
	// Check that we've written out the entire file for computing the sha
	if written != meta.Size {
		return errSizeMismatch
	}
	shaStr := hex.EncodeToString(hash.Sum(nil))
	if shaStr != meta.Oid {
		return errHashMismatch
	}
	input := s3.PutObjectInput{
		Bucket:        s.bucket.Name,
		ACL:           &s.acl,
		Key:           &path,
		ContentLength: &meta.Size,
		ContentType:   aws.String(ContentType),
		Body:          bytes.NewReader(buf),
	}
	retStat, err := s.client.PutObject(&input)

	// Block until the object has been written out
	s.client.WaitUntilObjectExists(&s3.HeadObjectInput{Bucket: s.bucket.Name, Key: &path})
	logger.Log(kv{"fn": "AwsContentStore.Put", "info": ": " + fmt.Sprint("%v", retStat)})
	if err != nil {
		logger.Log(kv{"fn": "AwsContentStore.Put", "error": ": " + err.Error()})
		return err
	}
	return nil
}

func (s *AwsContentStore) Exists(meta *MetaObject) bool {
	path := transformKey(meta.Oid)
	// returns a 404 error if its not there
	input := s3.GetObjectInput{Bucket: s.bucket.Name, Key: &path}
	_, err := s.client.GetObject(&input)
	if err != nil {
		if strings.Contains(err.Error(), "404") {
			return false
		} else {
			logger.Log(kv{"fn": "AwsContentStore.Exists", "error": ": " + err.Error()})
			return false
		}
	}
	// if the object is not there, a 404 error is raised
	return true
}

func (s *AwsContentStore) setAcl() {
	switch {
	case Config.Aws.BucketAcl == "private":
		s.acl = s3.BucketCannedACLPrivate
		return
	case Config.Aws.BucketAcl == "public-read":
		s.acl = s3.BucketCannedACLPublicRead
		return
	case Config.Aws.BucketAcl == "public-read-write":
		s.acl = s3.BucketCannedACLPublicReadWrite
		return
	case Config.Aws.BucketAcl == "authenticated-read":
		s.acl = s3.BucketCannedACLAuthenticatedRead
		return
	case Config.Aws.BucketAcl == "bucket-owner-full-control":
		s.acl = s3.ObjectCannedACLBucketOwnerFullControl
		return
	default:
		s.acl = s3.BucketCannedACLPrivate
		return
	}
	return
}
