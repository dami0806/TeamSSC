package com.sparta.teamssc.domain.image.service;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.sparta.teamssc.domain.image.entity.FileContentType;
import com.sparta.teamssc.domain.image.entity.Image;
import com.sparta.teamssc.domain.image.exception.ImageException;
import com.sparta.teamssc.domain.image.repository.ImageRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class ImageServiceImpl implements ImageService {

    private final AmazonS3 amazonS3;
    private final ImageRepository imageRepository;

    @Value("${S3_BUCKETNAME:dummy-bucket}")
    private String bucketName;

    // 파일을 S3에 업로드하고 파일 URL을 반환
    public Image uploadFile(MultipartFile file){
        try {
            // 파일 타입 검사
            String contentType = file.getContentType();
            if (FileContentType.getContentType(contentType) == null) {
                throw new IllegalArgumentException("허용되지 않는 파일 타입입니다: " + contentType);
            }

            String fileName = generateFileName(file.getOriginalFilename());

            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(file.getSize());
            metadata.setContentType(file.getContentType());

            amazonS3.putObject(bucketName, fileName, file.getInputStream(), metadata);

            String fileUrl = getFileUrl(fileName);

            // 이미지 엔티티를 생성하여 데이터베이스에 저장
            Image image = new Image(fileName, fileUrl);
            imageRepository.save(image);

            return image;
        }
        catch (IOException e) {
            throw new ImageException("이미지를 업로드할 수 없습니다. " + e.getMessage());
        }
    }

    // 파일을 S3에서 삭제하고 데이터베이스에서도 삭제하는 메서드
    public void deleteFile(String fileUrl) {
        try {
            Image image = imageRepository.findByFileLink(fileUrl);
            if (image != null) {
                amazonS3.deleteObject(bucketName, image.getName());
                imageRepository.delete(image);
            }
        }
        catch (Exception e) {
            throw new IllegalArgumentException("이미지을 삭제할 수 없습니다. " + e.getMessage());
        }
    }

    @Override
    public String updateProfileImage(MultipartFile file) {

        try {
            // 파일 타입 검사
            String contentType = file.getContentType();
            if (FileContentType.getContentType(contentType) == null) {
                throw new IllegalArgumentException("허용되지 않는 파일 타입입니다: " + contentType);
            }

            String fileName = generateFileName(file.getOriginalFilename());

            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(file.getSize());
            metadata.setContentType(file.getContentType());

            amazonS3.putObject(bucketName, fileName, file.getInputStream(), metadata);

            String fileUrl = getFileUrl(fileName);
            return fileUrl;
        }
        catch (IOException e) {
            throw new ImageException("이미지를 업로드할 수 없습니다. " + e.getMessage());
        }
    }

    // 파일명을 생성하는 메서드
    public String generateFileName(String originalFileName) {
        return UUID.randomUUID().toString() + "_" + originalFileName;
    }

    // 파일 URL을 반환하는 메서드
    public String getFileUrl(String fileName) {
        if (!amazonS3.doesObjectExist(bucketName, fileName)) {
            throw new IllegalArgumentException("이미지을 업로드할 수 없습니다.");
        }
        return amazonS3.getUrl(bucketName, fileName).toString();
    }

    public String findFileUrlByImageId(Long imageId) {

        Image image = imageRepository.findById(imageId).orElseThrow(() ->
                new IllegalArgumentException("해당 이미지를 찾을 수 없습니다"));

        return image.getFileLink();
    }
}
