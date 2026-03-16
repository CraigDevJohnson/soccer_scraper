package main

import (
	"context"
	"reflect"
	"testing"

	"github.com/CraigDevJohnson/soccer_scraper/internal/app"
	"github.com/CraigDevJohnson/soccer_scraper/internal/lps"
	"github.com/urfave/cli/v3"
)

func Test_main(t *testing.T) {
	tests := []struct {
		name string
	}{
		// TODO: #35 Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			main()
		})
	}
}

func Test_prepareClientAndTeams(t *testing.T) {
	type args struct {
		command *cli.Command
	}
	tests := []struct {
		name    string
		args    args
		want    *lps.Client
		want1   []string
		want2   []app.InvalidTeamID
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1, got2, err := prepareClientAndTeams(tt.args.command)
			if (err != nil) != tt.wantErr {
				t.Fatalf("prepareClientAndTeams() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("prepareClientAndTeams() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("prepareClientAndTeams() got1 = %v, want %v", got1, tt.want1)
			}
			if !reflect.DeepEqual(got2, tt.want2) {
				t.Errorf("prepareClientAndTeams() got2 = %v, want %v", got2, tt.want2)
			}
		})
	}
}

func Test_reportFailedTeams(t *testing.T) {
	type args struct {
		failedTeams []app.FailedTeam
	}
	tests := []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reportFailedTeams(tt.args.failedTeams)
		})
	}
}

func Test_fetchAction(t *testing.T) {
	type args struct {
		ctx     context.Context
		command *cli.Command
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := fetchAction(tt.args.ctx, tt.args.command); (err != nil) != tt.wantErr {
				t.Errorf("fetchAction() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_downloadAction(t *testing.T) {
	type args struct {
		ctx     context.Context
		command *cli.Command
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := downloadAction(tt.args.ctx, tt.args.command); (err != nil) != tt.wantErr {
				t.Errorf("downloadAction() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_subscribeAction(t *testing.T) {
	type args struct {
		ctx     context.Context
		command *cli.Command
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := subscribeAction(tt.args.ctx, tt.args.command); (err != nil) != tt.wantErr {
				t.Errorf("subscribeAction() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_checkChangesAction(t *testing.T) {
	type args struct {
		ctx     context.Context
		command *cli.Command
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := checkChangesAction(tt.args.ctx, tt.args.command); (err != nil) != tt.wantErr {
				t.Errorf("checkChangesAction() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
