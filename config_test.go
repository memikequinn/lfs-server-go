package main

import (
	"os"
	"testing"
)

var config *Configuration

func TestConfig(t *testing.T) {
	setupConfigTest()
	Config.Public = false
	if Config.IsPublic() {
		t.Fatalf("Expected IsPublic to be false but got '%v'", config.IsPublic())
	}

	Config.Scheme = "http"
	if Config.IsHTTPS() {
		t.Fatalf("Expected IsHTTPS to be false but got '%v'", config.IsHTTPS())
	}

	Config.Ldap.Enabled = false
	if Config.IsLdapEnabled() {
		t.Fatalf("Expected IsLdapEnabled to be false but got '%v'", config.IsLdapEnabled())
	}

}

func setupConfigTest() {
	config = &Configuration{}
	os.Setenv("LFS_SERVER_GO_CONFIG", "")
}
