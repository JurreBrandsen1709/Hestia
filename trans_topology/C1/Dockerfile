# Use ASP.NET runtime image
FROM mcr.microsoft.com/dotnet/aspnet:7.0
WORKDIR /app

# Copy publish output from local machine
COPY ./trans_topology/C1/out .
COPY ./trans_topology/C1/faulty_consumer.json /app/faulty_consumer.json

ENTRYPOINT ["dotnet", "Consumer.dll"]
