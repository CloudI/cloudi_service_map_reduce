#-*-Mode:elixir;coding:utf-8;tab-width:2;c-basic-offset:2;indent-tabs-mode:()-*-
# ex: set ft=elixir fenc=utf-8 sts=2 ts=2 sw=2 et nomod:

defmodule CloudIServiceMapReduce do
  use Mix.Project

  def project do
    [app: :cloudi_service_map_reduce,
     version: "1.7.2",
     language: :erlang,
     description: description(),
     package: package(),
     deps: deps()]
  end

  defp deps do
    [{:cloudi_core, "~> 1.7.2"}]
  end

  defp description do
    "Erlang/Elixir Cloud Framework Map/Reduce Service"
  end

  defp package do
    [files: ~w(src doc rebar.config README.markdown),
     maintainers: ["Michael Truog"],
     licenses: ["MIT"],
     links: %{"Website" => "http://cloudi.org",
              "GitHub" => "https://github.com/CloudI/" <>
                          "cloudi_service_map_reduce"}]
   end
end
