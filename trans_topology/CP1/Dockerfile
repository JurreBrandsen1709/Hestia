# Use ASP.NET runtime image
FROM mcr.microsoft.com/dotnet/aspnet:7.0
WORKDIR /app

# Copy publish output from local machine
COPY ./trans_topology/CP1/out .
COPY ./trans_topology/CP1/normal_consumer.json /app/normal_consumer.json

ENTRYPOINT ["dotnet", "Consumer.dll"]
