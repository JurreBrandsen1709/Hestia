# Use ASP.NET runtime image
FROM mcr.microsoft.com/dotnet/aspnet:7.0
WORKDIR /app

# Copy publish output from local machine
COPY ./star_topology/C4/out .
COPY ./star_topology/C4/normal_consumer.json /app/normal_consumer.json

ENTRYPOINT ["dotnet", "Consumer.dll"]
