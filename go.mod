module github.com/simonswine/covid-prometheus

go 1.14

require (
	github.com/go-kit/kit v0.10.0
	github.com/jszwec/csvutil v1.4.0
	github.com/prometheus/client_golang v1.7.1
	github.com/prometheus/common v0.10.0
	github.com/prometheus/prometheus v1.8.2-0.20200805170718-983ebb4a5133
	github.com/thanos-io/thanos v0.14.0
	k8s.io/api v0.18.8
	k8s.io/apimachinery v0.18.8
	k8s.io/client-go v12.0.0+incompatible
	k8s.io/klog v1.0.0
)

replace k8s.io/client-go => k8s.io/client-go v0.18.8
