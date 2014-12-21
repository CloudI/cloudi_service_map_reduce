defmodule CloudIServiceMapReduce do
  use Mix.Project

  def project do
    [app: :cloudi_service_map_reduce,
     version: "1.4.0",
     language: :erlang,
     description: description,
     package: package,
     deps: deps]
  end

  defp deps do
    [{:cloudi_core, "~> 1.4.0"}]
  end

  defp description do
    "Erlang/Elixir Cloud Framework Map/Reduce Service"
  end

  defp package do
    [files: ~w(src doc rebar.config README.markdown),
     contributors: ["Michael Truog"],
     licenses: ["BSD"],
     links: %{"Website" => "http://cloudi.org",
              "GitHub" => "https://github.com/CloudI/" <>
                          "cloudi_service_map_reduce"}]
   end
end